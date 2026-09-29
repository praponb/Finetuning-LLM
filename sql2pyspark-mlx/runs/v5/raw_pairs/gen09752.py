from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("distinct_ids").desc())

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("discount") >= 0.38, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("shipping_fee")).otherwise(0)).alias("partner_shipping_fee"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("amount_per_cancelled"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w))
    .orderBy("distinct_ids")
)
