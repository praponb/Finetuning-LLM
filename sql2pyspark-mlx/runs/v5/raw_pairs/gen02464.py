from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(~F.col("page").isin(["/home", "/product"])).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
    )
    .orderBy(F.col("country").desc())
)
