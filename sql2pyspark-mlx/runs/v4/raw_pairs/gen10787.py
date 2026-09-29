from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("partner_max_discount").desc())

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-21").cast("date")) & F.col("store_id").isNotNull())
    .withColumn("discount_per_amount", F.round((F.col("discount") / F.col("amount")), 2))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    filtered
    .groupBy(
        "channel",
        F.when(F.col("discount_per_amount") >= 66, "LARGE").otherwise("SMALL").alias("discount_per_amount_tier"),
    )
    .agg(
        F.max(F.when(F.col("channel") == "partner", F.col("discount"))).alias("partner_max_discount"),
        F.round(F.sum("discount_per_amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_per_amount_per_store"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .withColumn("partner_max_discount_percent_rank", F.percent_rank().over(w))
)
