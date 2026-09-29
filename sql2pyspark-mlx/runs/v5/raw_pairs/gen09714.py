from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") != "desktop") & ((F.col("country") == "DE") | (F.col("started_at") >= "2023-05-01")))
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        F.when(F.col("browser") == "Edge", 1).otherwise(0).alias("is_edge"),
    )
    .orderBy(F.col("id").desc(), "device")
)
