from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at_month").desc(), "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-07-15 05:00:00").cast("timestamp")) & F.col("order_id").isNotNull() & (F.col("method") != "cash"))
    .select(
        "*",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.month("paid_at").alias("paid_at_month"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
