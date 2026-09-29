from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy("discount", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-04-19").cast("date")) & F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "discount",
        "shipping_fee",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
        (F.col("shipping_fee") + F.col("discount")).alias("shipping_fee_plus_discount"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") >= "2025-02-05"), "left_anti")
)

metrics = (
    eligible
    .withColumn("next_discount_plus_amount", F.lead("discount_plus_amount", 1).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

with_dims = (
    metrics
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.age").between(46, 61))
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.status",
        "src.shipping_fee",
        "src.discount_plus_amount",
        "src.next_discount_plus_amount",
        F.col("p.segment"),
    )
)

with_dims_orders = (
    with_dims
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.shipping_fee",
        "a.discount_plus_amount",
        "a.next_discount_plus_amount",
        "a.segment",
        F.col("d.region"),
        F.col("d.opened_date"),
        F.col("d.sqft"),
    )
)

totals = (
    with_dims_orders
    .groupBy(F.when(F.col("sqft") >= 15465, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.percentile_approx("discount_plus_amount", 0.5).alias("discount_plus_amount_p50"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("shipping_fee")).otherwise(0)).alias("smb_shipping_fee"),
        F.percentile_approx("discount_plus_amount", [0.25, 0.5, 0.75]).alias("discount_plus_amount_quartiles"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
)

result = (
    totals
    .select(
        "sqft_tier",
        "discount_plus_amount_p50",
        "corporate_count",
        "discount_plus_amount_quartiles",
        "num_regions",
    )
)
