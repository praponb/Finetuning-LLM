from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("customer_id", "store_id").orderBy("amount")
w3 = Window.partitionBy("status")
w4 = Window.orderBy("amount_cume_dist", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("channel", "store_city").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("amount_bucket", "id")

with_windows = (
    df_orders
    .withColumn("amount_bucket", F.ntile(3).over(w))
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
    .withColumn("status_stddev_samp_shipping_fee", F.stddev_samp("shipping_fee").over(w3))
)

joined = (
    with_windows
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"), F.col("d.region"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([31, 37])), "left_anti")
)

enriched = (
    not_flagged
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.city").alias("store_city"))
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("amount").over(w4).alias("amount_avg_2"),
        F.sum("amount_bucket").over(w5).alias("running_amount_bucket"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "amount_bucket",
        "status_stddev_samp_shipping_fee",
        "city",
        "region",
        "store_city",
    )
    .orderBy(F.col("id").desc())
)
