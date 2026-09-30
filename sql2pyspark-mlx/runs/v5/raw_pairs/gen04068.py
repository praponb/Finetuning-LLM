from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy(F.col("amount").desc_nulls_last(), "id")

ranked = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("amount") >= 1760, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.max(F.when(F.col("method") == "bank_transfer", F.col("amount"))).alias("bank_transfer_max_amount"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)
