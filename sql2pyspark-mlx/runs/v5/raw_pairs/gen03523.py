from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id")
w2 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id", "store_id").orderBy(F.col("discount").desc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy(F.col("amount_per_shipping_fee").desc_nulls_first())
w5 = Window.partitionBy("customer_id", "channel").orderBy("running_amount_per_shipping_fee", "id")
w6 = Window.partitionBy("customer_id", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("units").alias("avg_store_sale_units"))

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-07-17").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        F.upper("status").alias("status_upper"),
        (F.col("amount") / F.nullif("shipping_fee", F.lit(0))).alias("amount_per_shipping_fee"),
    )
)

scored = (
    filtered
    .withColumn("channel_max_amount_per_shipping_fee", F.max("amount_per_shipping_fee").over(w))
    .withColumn("running_amount_per_shipping_fee", F.sum("amount_per_shipping_fee").over(w2))
    .withColumn("discount_rank", F.rank().over(w_part))
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        "status_upper",
        "amount_per_shipping_fee",
        "running_amount_per_shipping_fee",
        F.rank().over(w4).alias("amount_per_shipping_fee_rank"),
        F.ntile(10).over(w5).alias("running_amount_per_shipping_fee_bucket"),
        F.sum("channel_max_amount_per_shipping_fee").over(w6).alias("running_channel_max_amount_per_shipping_fee"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.region").isin(["West", "South", "East"]))
    .select(
        "src.id",
        "src.store_id",
        "src.order_date",
        "src.discount",
        "src.status_upper",
        "src.amount_per_shipping_fee",
        "src.amount_per_shipping_fee_rank",
        F.col("d.region"),
    )
)

by_type = (
    enriched
    .groupBy("store_id")
    .pivot("region", ["Central", "South"])
    .agg(F.avg("discount").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    by_type
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "Central_total",
        "Central_n",
        "South_total",
        F.col("avg_store_sale_units"),
    )
)
