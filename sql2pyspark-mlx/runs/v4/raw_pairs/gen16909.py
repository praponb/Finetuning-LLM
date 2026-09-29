from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_sqft", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id")
w4 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w5 = Window.partitionBy("region").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2024-02-11")))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        F.lower("channel").alias("channel_lower"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.opened_date"), F.col("b.sqft"), F.col("b.city"))
)

joined_orders = (
    joined
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.sqft").alias("store_sqft"), F.col("d.region"))
)

enriched = (
    joined_orders
    .withColumn("shipping_fee_avg_2_prior", F.avg("shipping_fee").over(w))
    .withColumn("sqft_bucket", F.ntile(10).over(w2))
    .withColumn("next_store_sqft", F.lead("store_sqft", 2).over(w3))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1).over(w4))
    .withColumn("cumulative_store_sqft", F.sum("store_sqft").over(w5))
)

enriched_orders = (
    enriched
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.store_name"))
)

result = (
    enriched_orders
    .select(
        "id",
        "order_date",
        "status",
        "shipping_fee",
        "channel_lower",
        "opened_date",
        "sqft",
        "store_sqft",
        "shipping_fee_avg_2_prior",
    )
    .orderBy(F.col("shipping_fee").asc_nulls_last())
)
