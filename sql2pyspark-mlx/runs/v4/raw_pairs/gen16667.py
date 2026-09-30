from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("shipping_fee_per_web").desc())

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-09-22").cast("date"))
    .withColumn("amount_with_shipping", F.round((F.col("shipping_fee") / F.col("amount")), 2))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("amount") >= 1750, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "status",
    )
    .agg(
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("shipping_fee_per_web"),
        F.percentile_approx("amount_with_shipping", [0.25, 0.5, 0.75]).alias("amount_with_shipping_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("shipping_fee_per_web") >= 0)
    .withColumn("shipping_fee_per_web_dense_rank", F.dense_rank().over(w))
)
