from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("rank_in_group", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.trim("channel").alias("clean_channel"),
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .select("id", "shipping_fee", "amount_with_shipping")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.shipping_fee", "s.amount_with_shipping", F.col("c.cost"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.method").isin(["bank_transfer", "paypal", "card"])), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("cost") >= 87, "LARGE").otherwise("SMALL").alias("cost_tier"),
        F.when(F.col("amount_with_shipping") >= 61, "HIGH").otherwise("LOW").alias("amount_with_shipping_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.percentile_approx("amount_with_shipping", [0.25, 0.5, 0.75]).alias("amount_with_shipping_quartiles"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1000)
    .orderBy(F.col("amount_with_shipping_tier").desc())
)
