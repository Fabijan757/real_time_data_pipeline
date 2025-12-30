import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px

st.set_page_config(page_title="LiveDB Dashboard", layout="wide")
st.title("LiveDB Dashboard (Kafka → Spark → MySQL)")

# ---- MySQL helper ----
def run_query(sql: str) -> pd.DataFrame:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="NovaLozinka",
        database="LiveDB"
    )
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# ---- KPI (gore) ----
kpi = run_query("""
SELECT
  COUNT(*) AS total_events,
  SUM(akcija='view') AS views,
  SUM(akcija='click') AS clicks,
  SUM(akcija='purchase') AS purchases
FROM events_stream;
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total events", int(kpi["total_events"]))
c2.metric("Views", int(kpi["views"]))
c3.metric("Clicks", int(kpi["clicks"]))
c4.metric("Purchases", int(kpi["purchases"]))

st.divider()

# ---- TOP 10 purchase ----
st.subheader("TOP 10 prodaja (purchase)")
df_sales = run_query("""
SELECT item, COUNT(*) AS broj
FROM events_stream
WHERE akcija='purchase'
GROUP BY item
ORDER BY broj DESC
LIMIT 10;
""")
st.plotly_chart(px.bar(df_sales, x="item", y="broj"), use_container_width=True)

# ---- TOP 10 click ----
st.subheader("️TOP 10 klikova (click)")
df_clicks = run_query("""
SELECT item, COUNT(*) AS broj
FROM events_stream
WHERE akcija='click'
GROUP BY item
ORDER BY broj DESC
LIMIT 10;
""")
st.plotly_chart(px.bar(df_clicks, x="item", y="broj"), use_container_width=True)

# ---- TOP 10 view ----
st.subheader("️ TOP 10 pregleda (view)")
df_views = run_query("""
SELECT item, COUNT(*) AS broj
FROM events_stream
WHERE akcija='view'
GROUP BY item
ORDER BY broj DESC
LIMIT 10;
""")
st.plotly_chart(px.bar(df_views, x="item", y="broj"), use_container_width=True)
