import streamlit as st

st.set_page_config(page_title="Data Health", page_icon="🩺", layout="wide")
conn = st.connection("snowflake")

st.title("Data health")

fresh = conn.query("""
    SELECT source, freshness_status, latest_event_utc, latest_load_at, data_lag_h, load_lag_h
    FROM PARALITRACE.OPS.SOURCE_FRESHNESS
    ORDER BY source
""", ttl=300)

statuses = set(fresh["FRESHNESS_STATUS"])
with st.container(border=True):
    st.markdown("#### 🗣️ In plain words")
    if statuses <= {"STALE", "NO_DATA"}:
        st.markdown("The dashboard is currently showing **past seasons (2023–2025) in replay mode**. "
                    "Live data collection isn't switched on yet, so 'Stale' below is expected, not a fault.")
    elif statuses == {"OK"}:
        st.markdown("**All three data sources are up to date.** Fire, air-quality and weather data are arriving on time.")
    else:
        late = ", ".join(fresh.loc[fresh["FRESHNESS_STATUS"] != "OK", "SOURCE"])
        st.markdown(f"**Some data is running late:** {late}. Charts may not include the most recent hours.")

st.subheader("Freshness")
icons = {"OK": "🟢", "WARN": "🟡", "STALE": "🔴", "NO_DATA": "⚫"}
cols = st.columns(len(fresh))
for col, row in zip(cols, fresh.itertuples()):
    col.metric(row.SOURCE, f"{icons.get(row.FRESHNESS_STATUS, '')} {row.FRESHNESS_STATUS}",
               f"data lag {row.DATA_LAG_H:,} h", delta_color="off")

st.subheader("Dead-letter queue")
dlq = conn.query("""
    SELECT source, error_reason, rejected_rows, clean_rows, reject_pct
    FROM PARALITRACE.OPS.DLQ_SUMMARY
    ORDER BY source, rejected_rows DESC
""", ttl=300)

per_src = dlq.groupby("SOURCE").agg(clean=("CLEAN_ROWS", "first"), rejected=("REJECTED_ROWS", "sum"))
rate = per_src["rejected"].sum() / (per_src["clean"].sum() + per_src["rejected"].sum()) * 1000
st.markdown(f"Out of every **1,000 records** we received, about **{rate:.1f}** had problems (missing or impossible values). "
            "Those weren't thrown away silently; they were set aside in a 'dead-letter' table so they can be checked.")

st.dataframe(dlq, hide_index=True, use_container_width=True)

st.subheader("Rows loaded per day")
loads = conn.query("""
    SELECT load_date, source, rows_loaded
    FROM PARALITRACE.OPS.LOAD_AUDIT
    ORDER BY load_date
""", ttl=300)
st.bar_chart(loads.pivot_table(index="LOAD_DATE", columns="SOURCE", values="ROWS_LOADED", aggfunc="sum"))