from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("shipping_fee_p95").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1320, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.when(F.col("discount") >= 0.36, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
        F.round(F.avg("discount"), 2).alias("avg_discount_2"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
    )
    .filter(F.col("shipping_fee_p95") < 0)
    .withColumn("shipping_fee_p95_rank", F.rank().over(w))
    .orderBy(F.col("avg_discount").asc_nulls_last())
)
