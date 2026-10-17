import streamlit as st

st.set_page_config(page_title="ParaliTrace", page_icon="🔥", layout="wide")
conn = st.connection("snowflake")

st.title("ParaliTrace")
st.caption("Tracing Punjab and Haryana farm fires to Delhi's air, 2023 to 2025")

st.markdown("""
Every October and November, farmers in Punjab and Haryana burn leftover rice straw to clear their fields
before the next crop. Satellites can see these fires from space. When the wind blows from the north-west,
the smoke can travel 300–400 km to Delhi in a day or two, just as cold, still winter air starts trapping
pollution over the city.

**This dashboard follows that smoke.** It shows where fires burned, which way the wind carried the smoke,
how polluted Delhi's air was, and how well we can predict tomorrow's air.
""")

with st.expander("What do these numbers mean?"):
    st.markdown("""
- **PM2.5**: tiny particles in the air, small enough to reach deep into the lungs. Measured in µg/m³.
  India's safe daily limit is **60**; the World Health Organization recommends **15**.
- **Air quality bands (India's NAQI)**: Good (0–30) · Satisfactory (31–60) · Moderate (61–90) ·
  Poor (91–120) · Very poor (121–250) · Severe (above 250).
- **Fire power (MW / GW)**: how much heat a fire gives off, measured by satellite. Bigger fires burn
  more straw and make more smoke. 1 GW = 1,000 MW.
- **Smoke score (smoke index)**: our estimate of how much farm smoke is reaching Delhi, based on fire size,
  distance and wind. Higher means more smoke. It's an estimate, not a direct measurement.
- **Forecast error**: how far, on average, our prediction was from the real reading.
""")

kpi = conn.query("""
    WITH fires AS (
        SELECT YEAR(hour_start_utc) AS season, COUNT(*) AS fire_events, SUM(frp_mw) AS total_frp_mw
        FROM PARALITRACE.SILVER.FCT_FIRE_DETECTION
        WHERE region_key IN ('punjab', 'haryana')
        GROUP BY YEAR(hour_start_utc)
    ),
    aq AS (
        SELECT season, AVG(pm25) AS avg_pm25, MAX(pm25) AS max_pm25
        FROM PARALITRACE.GOLD.MART_DELHI_PM25_VS_INDEX
        WHERE station_key = 'new_delhi_airnow' AND is_usable
        GROUP BY season
    )
    SELECT f.season, f.fire_events, f.total_frp_mw, a.avg_pm25, a.max_pm25
    FROM fires f
    LEFT JOIN aq a ON a.season = f.season
    WHERE f.season BETWEEN 2023 AND 2025
    ORDER BY f.season
""", ttl=3600)

st.subheader("Three burning seasons at a glance")
cols = st.columns(len(kpi))
for col, row in zip(cols, kpi.itertuples()):
    col.markdown(f"**Season {row.SEASON}**")
    col.metric("Farm fires spotted", f"{row.FIRE_EVENTS:,}")
    col.metric("Total fire power", f"{row.TOTAL_FRP_MW / 1000:,.0f} GW")
    col.metric("Delhi's average PM2.5", f"{row.AVG_PM25:,.0f} µg/m³",
               f"{row.AVG_PM25 / 60:.1f}× the safe limit", delta_color="off")

first, last = kpi.iloc[0], kpi.iloc[-1]
fire_drop = (1 - last["TOTAL_FRP_MW"] / first["TOTAL_FRP_MW"]) * 100

st.subheader("What we found")
with st.container(border=True):
    st.markdown(f"""
1. **Farm fires have dropped sharply.** The {int(last['SEASON'])} season had about **{fire_drop:.0f}% less fire power**
   than {int(first['SEASON'])}.
2. **Delhi's air is still unhealthy.** Even in {int(last['SEASON'])}, the average was **{last['AVG_PM25']:.0f} µg/m³**,
   about **{last['AVG_PM25'] / 60:.1f}× India's safe limit**. Farm fires are one cause, but traffic, dust, industry
   and cold, still winter air matter too.
3. **Our smoke score matches official estimates.** On the days India's official air-quality model said farm smoke
   mattered most, our score was high too.
4. **We can predict Delhi's air 1–2 days ahead** about **12% better** than simply assuming "tomorrow will be like today".
   Adding fire data didn't improve the forecast: by the time smoke matters, it already shows up in Delhi's air readings.
""")

with st.expander("Technical details"):
    st.markdown("""
- Smoke index vs IITM-DSS daily stubble-burning share: Spearman 0.85 (raw fire power: 0.64), n = 14 days, spot check.
- Forecast (LightGBM, test season 2025): 24 h MAE 36.3 vs persistence 41.4; 48 h MAE 42.0 vs 48.3.
- Fire-feature ablation: all 95% bootstrap CIs include zero across leave-one-season-out folds.
- The smoke index is a heuristic transport index, not atmospheric source attribution.
""")