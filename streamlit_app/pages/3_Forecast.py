import altair as alt
import streamlit as st
import numpy as np

st.set_page_config(page_title="Forecast", page_icon="📈", layout="wide")
conn = st.connection("snowflake")

st.title("PM2.5 forecast vs actual")
st.caption("New Delhi (AirNow 8118)")

# ---------- LIVE FORECAST (from Airflow / predict_live.py) ----------
st.subheader("🔴 Live forecast")
try:
    live = conn.query("""
        WITH latest AS (
            SELECT MAX(issue_hour_utc) AS t FROM PARALITRACE.ML.FORECAST_PM25_LIVE
        )
        SELECT f.horizon_h,
               CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', f.issue_hour_utc) AS issue_ist,
               CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', f.target_hour_utc) AS target_ist,
               f.pm25_now,
               f.pm25_pred
        FROM PARALITRACE.ML.FORECAST_PM25_LIVE f
        JOIN latest l ON f.issue_hour_utc = l.t
        ORDER BY f.horizon_h
    """, ttl=600)

    track = conn.query("""
        SELECT f.horizon_h, f.pm25_pred, a.pm25 AS pm25_actual
        FROM PARALITRACE.ML.FORECAST_PM25_LIVE f
        JOIN PARALITRACE.SILVER.FCT_AQ_HOURLY a
            ON a.hour_start_utc = f.target_hour_utc
           AND a.station_key = 'new_delhi_airnow'
           AND a.is_usable
        WHERE a.pm25 IS NOT NULL
    """, ttl=600)
except Exception:
    live, track = None, None

if live is None or live.empty:
    st.info("Live forecasts will appear here once the pipeline has run. The backtest below shows how the model performs.")
else:
    bands = [30, 60, 90, 120, 250]
    band_names = ["Good", "Satisfactory", "Moderate", "Poor", "Very poor", "Severe"]
    now_pm = live["PM25_NOW"].iloc[0]
    st.caption(f"Forecast made at {live['ISSUE_IST'].iloc[0]:%d %b, %H:%M} IST · "
               f"Delhi's PM2.5 at that time: {now_pm:.0f} µg/m³")

    cols = st.columns(len(live))
    for col, r in zip(cols, live.itertuples()):
        band = band_names[int(np.digitize(r.PM25_PRED, bands, right=True))]
        col.metric(f"In {r.HORIZON_H} hours ({r.TARGET_IST:%a %d %b, %H:%M})",
                   f"{r.PM25_PRED:.0f} µg/m³")
        col.caption(f"Air quality band: **{band}**")

    if track is not None and len(track):
        t24 = track[track["HORIZON_H"] == 24]
        if len(t24):
            mae_live = (t24["PM25_ACTUAL"] - t24["PM25_PRED"]).abs().mean()
            st.caption(f"Live track record so far (24 h ahead): off by {mae_live:.0f} µg/m³ on average, "
                       f"across {len(t24)} forecasts that can now be checked against real readings.")

st.divider()
st.subheader("How well the model did on past seasons")

c1, c2 = st.columns(2)
season = c1.selectbox("Season", [2025, 2024, 2023])
horizon = c2.radio("Forecast horizon", [24, 48], format_func=lambda h: f"{h} hours ahead", horizontal=True)

if season == 2025:
    st.success("2025 is the honest test: the model was trained on 2023–2024 and never saw this season.")
else:
    st.info(f"{season} is cross-validated: the model was trained on the other two seasons, "
            "including later ones. Use it to see behaviour, not as the headline score.")

df = conn.query(f"""
    SELECT
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', DATEADD(HOUR, horizon_h, issue_hour_utc)) AS target_time_ist,
        pm25_actual,
        pm25_pred,
        pm25_persistence
    FROM PARALITRACE.ML.FORECAST_PM25_BACKTEST
    WHERE horizon_h = {horizon} AND test_season = {season}
    ORDER BY target_time_ist
""", ttl=3600)

mae_model = (df["PM25_ACTUAL"] - df["PM25_PRED"]).abs().mean()
mae_pers = (df["PM25_ACTUAL"] - df["PM25_PERSISTENCE"]).abs().mean()

m1, m2, m3 = st.columns(3)
m1.metric("Model MAE", f"{mae_model:.1f} µg/m³")
m2.metric("Persistence MAE", f"{mae_pers:.1f} µg/m³")
m3.metric("Improvement", f"{(1 - mae_model / mae_pers) * 100:.1f}%")

bins_f = [30, 60, 90, 120, 250]
cat_actual = np.digitize(df["PM25_ACTUAL"], bins_f, right=True)
cat_pred = np.digitize(df["PM25_PRED"], bins_f, right=True)
exact = (cat_actual == cat_pred).mean() * 100
within_one = (np.abs(cat_actual - cat_pred) <= 1).mean() * 100

with st.container(border=True):
    st.markdown("#### 🗣️ In plain words")
    st.markdown(f"""
We predict how polluted Delhi's air will be **{'tomorrow (24 hours ahead)' if horizon == 24 else 'the day after tomorrow (48 hours ahead)'}**.

On average, the forecast is off by about **{mae_model:.0f} µg/m³**. A simple guess, *"it'll be the same as now"*,
is off by **{mae_pers:.0f}**, so our model is **{(1 - mae_model / mae_pers) * 100:.0f}% more accurate** than that guess.

In everyday terms, the forecast put Delhi's air in the **exact right quality band** (Good, Moderate, Poor and so on)
**{exact:.0f}%** of the time, and **within one band {within_one:.0f}%** of the time. It struggles most with sudden jumps,
such as Diwali night or a sudden change in the weather.
""")

min_d, max_d = df["TARGET_TIME_IST"].min().date(), df["TARGET_TIME_IST"].max().date()
start, end = st.slider("Date range", min_value=min_d, max_value=max_d, value=(min_d, max_d))
view = df[(df["TARGET_TIME_IST"].dt.date >= start) & (df["TARGET_TIME_IST"].dt.date <= end)]

long = view.rename(columns={
    "PM25_ACTUAL": "Actual", "PM25_PRED": "Model", "PM25_PERSISTENCE": "Persistence"
}).melt(id_vars="TARGET_TIME_IST", var_name="series", value_name="pm25")

pick = alt.selection_point(fields=["series"], bind="legend")

chart = (
    alt.Chart(long)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("TARGET_TIME_IST:T", title=None, axis=alt.Axis(format="%d %b", labelAngle=0)),
        y=alt.Y("pm25:Q", title="PM2.5 (µg/m³)"),
        color=alt.Color(
            "series:N",
            scale=alt.Scale(domain=["Actual", "Model", "Persistence"],
                            range=["#3E8FD9", "#E06A2E", "#9C7FD6"]),
            legend=alt.Legend(title="Click to isolate · double-click chart to reset", orient="top"),
        ),
        strokeDash=alt.condition(alt.datum.series == "Persistence", alt.value([5, 4]), alt.value([1, 0])),
        opacity=alt.condition(pick, alt.value(1), alt.value(0.08)),
        tooltip=[
            alt.Tooltip("TARGET_TIME_IST:T", title="Time (IST)", format="%d %b %H:%M"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("pm25:Q", title="PM2.5", format=".0f"),
        ],
    )
    .add_params(pick)
    .properties(height=440)
)

st.altair_chart(chart, use_container_width=True)
st.caption("Backtest uses observed weather at target time as a stand-in for a weather forecast.")