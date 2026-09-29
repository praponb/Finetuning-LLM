from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount_rounded", "id")
w_part = Window.partitionBy("status", "method").orderBy("amount", "id")
w_part_2 = Window.partitionBy("method", "status").orderBy("amount", "id")

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .withColumn("amount_bucket", F.ntile(5).over(w_part))
    .withColumn("next_amount", F.lead("amount", 2).over(w_part_2))
)

by_type = (
    scored
    .groupBy("method")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.max("amount").alias("amount"))
    .select(
        "method",
        F.col("FAILED").alias("failed"),
        F.col("REFUNDED").alias("refunded"),
    )
)

result = (
    by_type
    .select("method")
    .orderBy(F.col("method").asc_nulls_last())
)
