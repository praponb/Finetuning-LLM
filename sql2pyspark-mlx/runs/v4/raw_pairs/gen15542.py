from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel").orderBy("least_shipping_fee_order_date_weekofyear", "id")
w2 = Window.partitionBy("store_id", "channel").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy("discount", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("amount") > 810))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.weekofyear("order_date").alias("order_date_weekofyear"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

base_orders = (
    base
    .filter(F.month("order_date").isin([5, 8, 11]))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_weekofyear",
        F.least("shipping_fee", "order_date_weekofyear").alias("least_shipping_fee_order_date_weekofyear"),
    )
)

metrics = (
    base_orders
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("discount_bucket", F.ntile(3).over(w2))
    .withColumn("amount_avg_3", F.avg("amount").over(w3))
    .withColumn("store_id_stddev_samp_discount", F.stddev_samp("discount").over(w_grp))
    .withColumn("prev_order_date_weekofyear", F.lag("order_date_weekofyear", 1).over(w5))
)

with_last = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

result = (
    with_last
    .rollup("store_id")
    .agg(
        F.round(F.sum("order_date_weekofyear") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("order_date_weekofyear_per_web"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_bucket")).otherwise(0)).alias("web_discount_bucket"),
    )
    .orderBy(F.col("order_date_weekofyear_per_web").asc_nulls_first())
)
