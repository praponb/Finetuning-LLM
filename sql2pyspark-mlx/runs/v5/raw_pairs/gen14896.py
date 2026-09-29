from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.unique_sessions") > 7)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("st.unique_sessions"),
        F.col("st.pct_product"),
    )
    .orderBy("pct_product")
)
