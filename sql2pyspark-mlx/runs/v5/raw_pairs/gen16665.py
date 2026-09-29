from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.hour("started_at").between(9, 15))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.trim("device").alias("clean_device"),
    )
)

result = (
    cleaned
    .groupBy("country")
    .agg(
        F.max("started_at").alias("ended_at"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
    .orderBy(F.col("jp_rows").asc_nulls_last())
)
