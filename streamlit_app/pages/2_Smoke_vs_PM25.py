import altair as alt
import streamlit as st

st.set_page_config(page_title="Smoke vs PM2.5", page_icon="🌫️", layout="wide")
conn = st.connection("snowflake")

st.title("Smoke index vs Delhi PM2.5")
season = st.selectbox("Season", [2025, 2024, 2023])

daily = conn.query(f"""
    SELECT s.date_ist,
           AVG(s.smoke_index) AS smoke_index,
           SUM(s.smoke_arriving_punjab) AS punjab,
           SUM(s.smoke_arriving_haryana) AS haryana,
           AVG(IFF(m.is_usable, m.pm25, NULL)) AS pm25
    FROM PARALITRACE.GOLD.FCT_SMOKE_INDEX_HOURLY s
    LEFT JOIN PARALITRACE.GOLD.MART_DELHI_PM25_VS_INDEX m
        ON m.hour_start_utc = s.hour_start_utc AND m.station_key = 'new_delhi_airnow'
    WHERE s.season = {season}
    GROUP BY s.date_ist
    ORDER BY s.date_ist
""", ttl=3600)

pm_daily = daily.dropna(subset=["PM25"])
severe_days = int((pm_daily["PM25"] > 250).sum())
very_poor_days = int(((pm_daily["PM25"] > 120) & (pm_daily["PM25"] <= 250)).sum())
pk_smoke = daily.loc[daily["SMOKE_INDEX"].idxmax()]
pk_pm = pm_daily.loc[pm_daily["PM25"].idxmax()]
gap = (pk_pm["DATE_IST"] - pk_smoke["DATE_IST"]).days

with st.container(border=True):
    st.markdown("#### 🗣️ In plain words")
    st.markdown(f"""
The **orange chart** shows how much farm smoke we estimate was reaching Delhi each day. The **blue chart**
shows how polluted Delhi's air actually was.

In {season}, Delhi had **{severe_days} 'Severe' days** and **{very_poor_days} 'Very poor' days**.
Farm smoke peaked on **{pk_smoke['DATE_IST']:%d %B}**, and Delhi's worst air came on **{pk_pm['DATE_IST']:%d %B}**
({abs(gap)} day{'s' if abs(gap) != 1 else ''} {'later' if gap >= 0 else 'earlier'}).

When the two charts rise together, farm smoke is likely part of the problem. When Delhi's air is bad but
smoke is low, other causes are to blame: traffic, dust, festivals like Diwali, and cold, still nights
that trap pollution near the ground.
""")

hover = alt.selection_point(fields=["DATE_IST"], on="pointerover", nearest=True, empty=False)
base = alt.Chart(daily).encode(x=alt.X("DATE_IST:T", title=None))

smoke_chart = (
    base.mark_area(color="#E06A2E", opacity=0.55, line={"color": "#E06A2E", "strokeWidth": 2})
    .encode(y=alt.Y("SMOKE_INDEX:Q", title="Smoke index"),
            tooltip=[alt.Tooltip("DATE_IST:T", title="Date"),
                     alt.Tooltip("SMOKE_INDEX:Q", title="Smoke index", format=",.0f"),
                     alt.Tooltip("PM25:Q", title="PM2.5", format=".0f")])
    .add_params(hover)
    .properties(height=220)
)
pm_chart = (
    base.mark_line(color="#3E8FD9", strokeWidth=2)
    .encode(y=alt.Y("PM25:Q", title="PM2.5 (µg/m³)"),
            tooltip=[alt.Tooltip("DATE_IST:T", title="Date"),
                     alt.Tooltip("PM25:Q", title="PM2.5", format=".0f")])
    .properties(height=220)
)
severe = alt.Chart().mark_rule(color="#8E24AA", strokeDash=[4, 4]).encode(y=alt.datum(250))
rule = base.mark_rule(color="#E6E4E0", opacity=0.4).encode(opacity=alt.condition(hover, alt.value(0.5), alt.value(0)))

st.altair_chart(smoke_chart.properties(height=220), use_container_width=True)
st.altair_chart(pm_chart.properties(height=220), use_container_width=True)
st.caption("Daily averages. Dashed line: NAQI 'Severe' PM2.5 threshold (250 µg/m³).")

st.subheader("Where the smoke came from")
src = daily.melt(id_vars="DATE_IST", value_vars=["PUNJAB", "HARYANA"], var_name="region", value_name="smoke")
st.altair_chart(
    alt.Chart(src).mark_area(opacity=0.85).encode(
        x=alt.X("DATE_IST:T", title=None),
        y=alt.Y("smoke:Q", stack=True, title="Smoke arriving (weighted)"),
        color=alt.Color("region:N", scale=alt.Scale(domain=["PUNJAB", "HARYANA"], range=["#E06A2E", "#9C7FD6"]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=["DATE_IST:T", "region:N", alt.Tooltip("smoke:Q", format=",.0f")],
    ).properties(height=240),
    use_container_width=True,
)
st.caption("Regions use bounding boxes; western Haryana falls in the Punjab box, so the split is approximate.")

st.subheader("Validation against IITM-DSS stubble-burning share")
val = conn.query("""
    WITH idx AS (
        SELECT date_ist, season, AVG(smoke_index_log) AS idx, AVG(LN(1 + upwind_frp_24h_naive)) AS naive
        FROM PARALITRACE.GOLD.FCT_SMOKE_INDEX_HOURLY
        GROUP BY date_ist, season
    ),
    ranked AS (
        SELECT *,
               ROUND(PERCENT_RANK() OVER (PARTITION BY season ORDER BY idx) * 100) AS idx_pctile,
               ROUND(PERCENT_RANK() OVER (PARTITION BY season ORDER BY naive) * 100) AS naive_pctile
        FROM idx
    )
    SELECT b.date_ist, b.stubble_share_pct, r.idx_pctile, r.naive_pctile, b.source
    FROM PARALITRACE.SILVER.DSS_STUBBLE_SHARE b
    JOIN ranked r ON r.date_ist = b.date_ist
    ORDER BY b.stubble_share_pct DESC
""", ttl=3600)

v1, v2 = st.columns(2)
v1.metric("Smoke index vs DSS (Spearman)", f"{val['STUBBLE_SHARE_PCT'].corr(val['IDX_PCTILE'], method='spearman'):.2f}")
v2.metric("Raw fire power vs DSS (Spearman)", f"{val['STUBBLE_SHARE_PCT'].corr(val['NAIVE_PCTILE'], method='spearman'):.2f}")
st.markdown("We checked our smoke score against **IITM's official air-quality model**, which estimates what share "
            "of Delhi's pollution comes from farm fires. On the days the official model said farm smoke mattered most, "
            "our score was high too. A score close to 1 means the two agree on which days were worst.")
st.dataframe(val, hide_index=True, use_container_width=True)
st.caption(f"n = {len(val)} days compiled from news reports quoting DSS. A spot check, not a full validation. "
           "DSS is itself a model.")