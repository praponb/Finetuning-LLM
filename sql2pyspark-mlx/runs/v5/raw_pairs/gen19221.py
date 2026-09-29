from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_p50").desc())

result = (
    df_orders
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.34, "HIGH").otherwise("LOW").alias("discount_tier"),
        "status",
    )
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("amount_per_partner"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("discount_per_returned"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("discount_per_returned") < 1000000)
    .withColumn("discount_p50_percent_rank", F.percent_rank().over(w))
    .orderBy("status")
)
