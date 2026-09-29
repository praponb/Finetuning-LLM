from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at_hour").desc_nulls_first(), "id")

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-16 16:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "started_at", "country", "started_at_hour", "resolved_country")
)
