import os
import numpy as np
import pandas as pd
import lightgbm as lgb
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sklearn.metrics import mean_absolute_error, mean_squared_error
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database="PARALITRACE",
    schema="GOLD",
)

df = conn.cursor().execute(
    "select * from ML_FEATURES_HOURLY where split in ('train', 'test')"
).fetch_pandas_all()
df.columns = df.columns.str.lower()

for c in ["is_peak_burning", "is_night_ist", "is_diwali_window"]:
    df[c] = df[c].astype(float)

BASE = [
    "hour_of_day_ist", "day_of_week",
    "is_peak_burning", "is_night_ist", "is_diwali_window",
    "pm25_now", "pm25_lag1", "pm25_lag3", "pm25_lag6", "pm25_lag24",
    "pm25_mean_24h", "pm25_max_24h",
    "blh", "ws10", "temp", "rh", "precip", "ventilation",
    "blh_min_24h", "precip_24h", "upwind_align", "upwind_ws100",
]
WX_FUTURE = {
    24: ["blh_at_24h", "ventilation_at_24h"],
    48: ["blh_at_48h", "ventilation_at_48h"],
}
FIRE = ["smoke_index_log_lag6", "smoke_index_log_lag12", "naive_log_lag3",
        "smoke_transit_log_24h", "smoke_transit_log_48h"]
VARIANTS = {
    "full":         FIRE,
    "no_fire":      [],
    "index_only":   ["smoke_index_log_lag6", "smoke_index_log_lag12"],
    "naive_only":   ["naive_log_lag3"],
    "transit_only": ["smoke_transit_log_24h", "smoke_transit_log_48h"],
}
PARAMS = dict(
    objective="regression", learning_rate=0.03, num_leaves=31,
    min_data_in_leaf=40, feature_fraction=0.8, bagging_fraction=0.8,
    bagging_freq=1, lambda_l2=1.0, verbose=-1, seed=42,
)

results = []
predictions = []
boot_rows = []
rng = np.random.default_rng(42)

for h in [24, 48]:
    target = f"target_pm25_{h}h"
    d = df[df[target].notna()]

    for test_season in [2023, 2024, 2025]:
        train = d[d["season"] != test_season]
        test = d[(d["season"] == test_season) & d["pm25_now"].notna()]
        peak = (test["is_peak_burning"] == 1).values
        y = test[target].values
        train_seasons = sorted(train["season"].unique())
        fit = train[train["season"] == train_seasons[0]]
        val = train[train["season"] == train_seasons[1]]

        abs_err = {}

        pred = test["pm25_now"].values
        abs_err["persistence"] = np.abs(y - pred)
        results.append(dict(horizon_h=h, test_season=test_season, model="persistence",
                            mae=mean_absolute_error(y, pred),
                            rmse=np.sqrt(mean_squared_error(y, pred)),
                            mae_peak=mean_absolute_error(y[peak], pred[peak])))

        for name, fire_cols in VARIANTS.items():
            feats = BASE + WX_FUTURE[h] + fire_cols

            probe = lgb.train(
                PARAMS,
                lgb.Dataset(fit[feats], np.log1p(fit[target])),
                num_boost_round=3000,
                valid_sets=[lgb.Dataset(val[feats], np.log1p(val[target]))],
                callbacks=[lgb.early_stopping(100, verbose=False)],
            )
            model = lgb.train(PARAMS, lgb.Dataset(train[feats], np.log1p(train[target])),
                              probe.best_iteration)
            pred = np.expm1(model.predict(test[feats]))
            abs_err[name] = np.abs(y - pred)

            results.append(dict(horizon_h=h, test_season=test_season, model=name,
                                mae=mean_absolute_error(y, pred),
                                rmse=np.sqrt(mean_squared_error(y, pred)),
                                mae_peak=mean_absolute_error(y[peak], pred[peak])))

            if name == "full" and test_season == 2023:
                imp = pd.Series(model.feature_importance("gain"), index=feats)
                print(f"\nTop features, {h}h, full, test 2023:")
                print((imp / imp.sum() * 100).sort_values(ascending=False).head(10).round(1))

            if name == "no_fire":
                predictions.append(pd.DataFrame({
                    "TEST_SEASON": test_season,
                    "ISSUE_HOUR_UTC": test["issue_hour_utc"].values,
                    "HORIZON_H": h,
                    "PM25_ACTUAL": y,
                    "PM25_PRED": pred,
                    "PM25_PERSISTENCE": test["pm25_now"].values,
                }))

        # day-block bootstrap: is the fire gain real?
        days = test["date_ist"].values
        for label, a, b in [("full_vs_no_fire", "no_fire", "full"),
                            ("transit_vs_no_fire", "no_fire", "transit_only")]:
            gain = pd.Series(abs_err[a] - abs_err[b]).groupby(days)
            day_sum, day_n = gain.sum().values, gain.size().values
            idx = rng.integers(0, len(day_sum), (2000, len(day_sum)))
            boot = day_sum[idx].sum(axis=1) / day_n[idx].sum(axis=1)
            lo, hi = np.percentile(boot, [2.5, 97.5])
            boot_rows.append(dict(horizon_h=h, test_season=test_season,
                                  comparison=label,
                                  fire_gain_mae=round(day_sum.sum() / day_n.sum(), 2),
                                  ci_low=round(lo, 2), ci_high=round(hi, 2)))

res = pd.DataFrame(results)
print("\nMAE by test season:")
print(res.pivot_table(index=["horizon_h", "model"], columns="test_season",
                      values="mae", sort=False).round(1).to_string())
print("\nPeak-weeks MAE by test season:")
print(res.pivot_table(index=["horizon_h", "model"], columns="test_season",
                      values="mae_peak", sort=False).round(1).to_string())
print("\nFire-feature gain (MAE reduction, positive = fire helps), 95% CI:")
print(pd.DataFrame(boot_rows).to_string(index=False))
# ---------- production models: train on all seasons, save for predict_live.py ----------
MODEL_DIR = Path(__file__).resolve().parent / "models"
MODEL_DIR.mkdir(exist_ok=True)

for h in [24, 48]:
    target = f"target_pm25_{h}h"
    d = df[df[target].notna()]
    feats = BASE + WX_FUTURE[h] + VARIANTS["no_fire"]

    # pick the number of trees on 2025 (unseen), then refit on all three seasons
    fit = d[d["season"] <= 2024]
    val = d[d["season"] == 2025]
    probe = lgb.train(
        PARAMS,
        lgb.Dataset(fit[feats], np.log1p(fit[target])),
        num_boost_round=3000,
        valid_sets=[lgb.Dataset(val[feats], np.log1p(val[target]))],
        callbacks=[lgb.early_stopping(100, verbose=False)],
    )
    final = lgb.train(PARAMS, lgb.Dataset(d[feats], np.log1p(d[target])), probe.best_iteration)
    final.save_model(str(MODEL_DIR / f"forecast_{h}h.txt"))
    print(f"Saved forecast_{h}h.txt: {probe.best_iteration} trees, trained on 2023-2025")

write_pandas(
    conn, pd.concat(predictions, ignore_index=True), "FORECAST_PM25_BACKTEST",
    database="PARALITRACE", schema="ML",
    auto_create_table=True, overwrite=True, use_logical_type=True,
)
print("\nPredictions written to PARALITRACE.ML.FORECAST_PM25_BACKTEST")
conn.close()