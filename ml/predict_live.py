import os
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()
MODEL_DIR = Path(__file__).resolve().parent / "models"
LOOKBACK_DAYS = 7

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database="PARALITRACE",
    schema="ML",
)

df = conn.cursor().execute(f"""
    SELECT *
    FROM PARALITRACE.GOLD.ML_FEATURES_HOURLY
    WHERE split = 'live'
      AND pm25_now IS NOT NULL
      AND issue_hour_utc >= DATEADD(DAY, -{LOOKBACK_DAYS}, SYSDATE())
""").fetch_pandas_all()
df.columns = df.columns.str.lower()

if df.empty:
    print("No live feature rows with a current PM2.5 reading yet. Nothing to forecast.")
    conn.close()
    raise SystemExit(0)

for c in ["is_peak_burning", "is_night_ist", "is_diwali_window"]:
    df[c] = df[c].astype(float)

predicted_at = pd.Timestamp.now(tz="UTC").tz_localize(None)
frames = []
for h in [24, 48]:
    model = lgb.Booster(model_file=str(MODEL_DIR / f"forecast_{h}h.txt"))
    feats = model.feature_name()
    pred = np.expm1(model.predict(df[feats]))

    missing_wx = df[f"blh_at_{h}h"].isna().mean() * 100
    print(f"{h}h: {len(df)} issue hours forecast, latest = {df['issue_hour_utc'].max()}, "
          f"weather-at-target missing for {missing_wx:.0f}%")

    frames.append(pd.DataFrame({
        "ISSUE_HOUR_UTC": df["issue_hour_utc"].values,
        "HORIZON_H": h,
        "TARGET_HOUR_UTC": (df["issue_hour_utc"] + pd.Timedelta(hours=h)).values,
        "PM25_NOW": df["pm25_now"].values,
        "PM25_PRED": pred,
        "MODEL_FILE": f"forecast_{h}h.txt",
        "PREDICTED_AT": predicted_at,
    }))

preds = pd.concat(frames, ignore_index=True)

cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS PARALITRACE.ML.FORECAST_PM25_LIVE (
        issue_hour_utc TIMESTAMP_NTZ,
        horizon_h INTEGER,
        target_hour_utc TIMESTAMP_NTZ,
        pm25_now FLOAT,
        pm25_pred FLOAT,
        model_file STRING,
        predicted_at TIMESTAMP_NTZ
    )
""")

write_pandas(conn, preds, "FORECAST_PM25_LIVE_STAGE", database="PARALITRACE", schema="ML",
             auto_create_table=True, overwrite=True, use_logical_type=True)

# Insert-only: a forecast is frozen as first made, so later runs (which see observed
# rather than forecast weather at the target hour) can't overwrite it with hindsight.
cur.execute("""
    MERGE INTO PARALITRACE.ML.FORECAST_PM25_LIVE t
    USING PARALITRACE.ML.FORECAST_PM25_LIVE_STAGE s
        ON t.issue_hour_utc = s.issue_hour_utc AND t.horizon_h = s.horizon_h
    WHEN NOT MATCHED THEN INSERT
        (issue_hour_utc, horizon_h, target_hour_utc, pm25_now, pm25_pred, model_file, predicted_at)
    VALUES
        (s.issue_hour_utc, s.horizon_h, s.target_hour_utc, s.pm25_now, s.pm25_pred, s.model_file, s.predicted_at)
""")
inserted = cur.fetchone()[0]
print(f"Inserted {inserted} new forecast rows into PARALITRACE.ML.FORECAST_PM25_LIVE "
      f"({len(preds) - inserted} already existed and were kept as first made)")
conn.close()