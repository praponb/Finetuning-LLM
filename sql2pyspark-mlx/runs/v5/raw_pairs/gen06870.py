from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.device") != "desktop")
    .select(
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.is_active"),
        F.lower(F.col("c.segment")).alias("segment_lower"),
    )
    .orderBy("device")
)
