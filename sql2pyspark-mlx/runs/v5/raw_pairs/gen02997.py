from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/search"]))
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("st.max_duration_sec"),
        F.col("st.unique_sessions"),
    )
)
