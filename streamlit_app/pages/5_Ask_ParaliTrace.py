import re
import streamlit as st
from google import genai
from google.genai import types

st.set_page_config(page_title="Ask ParaliTrace", page_icon="💬", layout="wide")
ro = st.connection("snowflake_ro", type="snowflake")
client = genai.Client(api_key=st.secrets["GEMINI_API_KEY"])
MODEL = st.secrets["GEMINI_MODEL"]

st.title("Ask ParaliTrace")
st.caption("Questions are turned into read-only SQL on the GOLD layer. Try: "
           "*Which day in 2024 had the most fire power in Punjab?* · "
           "*Average Delhi PM2.5 per season?* · *Top 5 days by smoke index in 2023*")

schema = ro.query("""
    SELECT table_name,
           LISTAGG(column_name || ' ' || data_type, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS cols
    FROM PARALITRACE.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'GOLD'
    GROUP BY table_name
""", ttl=86400)
schema_text = "\n".join(f"PARALITRACE.GOLD.{r.TABLE_NAME}({r.COLS})" for r in schema.itertuples())

SYSTEM = f"""You write Snowflake SQL for ParaliTrace, which links Punjab/Haryana crop fires to Delhi PM2.5.
Return exactly ONE read-only SELECT statement (WITH is allowed). No markdown, no explanation, no semicolon.
Use only these tables, fully qualified:
{schema_text}
Facts:
- Columns ending in _utc are UTC; date_ist is the Indian calendar date. Seasons are 2023, 2024, 2025 (Sep 15 to Dec 15).
- Delhi's main station is station_key = 'new_delhi_airnow'. When using pm25 add is_usable = TRUE.
- region_key values: 'punjab', 'haryana', 'delhi_ncr', 'other'. frp is fire radiative power in MW.
- smoke_index is a heuristic transport-weighted smoke score, not a measured concentration.
- Return at most 200 rows unless the query aggregates.
- Prefer the simplest query that answers the question. Use the same SQL for the same question.
- FCT_SMOKE_INDEX_HOURLY, MART_DELHI_PM25_VS_INDEX and ML_FEATURES_HOURLY have one row per HOUR. When the question says "day", "daily" or "date", aggregate to one row per day: convert to IST with CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hour_start_utc)::DATE AS date_ist, use MAX() or AVG() of the measure, and GROUP BY date_ist.
- MART_FIRES_DAILY_BY_REGION is already one row per day per region; never re-aggregate it by hour.
- Never return duplicate dates when the user asks for top N days.
"""

BLOCKED = r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|CALL|COPY|PUT|GET|USE|EXECUTE)\b"

if "chat" not in st.session_state:
    st.session_state.chat = []

for turn in st.session_state.chat:
    with st.chat_message("user"):
        st.write(turn["q"])
    with st.chat_message("assistant"):
        if turn.get("error"):
            st.error(turn["error"])
        else:
            st.write(turn["answer"])
            st.dataframe(turn["df"], hide_index=True, use_container_width=True)
        with st.expander("SQL"):
            st.code(turn["sql"], language="sql")

q = st.chat_input("Ask about fires, smoke or Delhi PM2.5…")
if q:
    turn = {"q": q, "sql": ""}
    try:
        resp = client.models.generate_content(
            model=MODEL, contents=q,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM,
                thinking_config=types.ThinkingConfig(thinking_level="low"),
            ),
        )
        sql = resp.text or ""
        sql = re.sub(r"^```(sql)?|```$", "", sql.strip(), flags=re.IGNORECASE | re.MULTILINE).strip().rstrip(";")
        turn["sql"] = sql

        if ";" in sql or not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE) or re.search(BLOCKED, sql, re.IGNORECASE):
            raise ValueError("Only a single read-only SELECT is allowed. Try rephrasing the question.")

        df = ro.query(f"SELECT * FROM ({sql}) LIMIT 500", ttl=0)
        turn["df"] = df

        turn["answer"] = client.models.generate_content(
            model=MODEL,
            contents=f"Question: {q}\nSQL result (first rows):\n{df.head(50).to_csv(index=False)}",
            config=types.GenerateContentConfig(
                system_instruction="Answer the question in 2-3 plain sentences using only the result given. "
                                   "Include the key numbers with units. If the result is empty, say so.",
                thinking_config=types.ThinkingConfig(thinking_level="minimal"),
            ),
        ).text or "No answer returned."
    except Exception as e:
        turn["error"] = f"Couldn't answer that: {e}"
    st.session_state.chat.append(turn)
    st.rerun()