from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("quantity_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_adjusted"),
    )
)

with_windows = (
    filtered
    .filter(F.col("product_id").isin([10, 16, 26]))
    .withColumn("running_min_quantity", F.min("quantity").over(w_hist))
    .withColumn("running_count_quantity", F.count("quantity").over(w_hist))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w_hist))
    .withColumn("running_sum_quantity", F.sum("quantity").over(w_hist))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
)

enriched = (
    with_windows
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.shipping_fee"), F.col("o.channel"))
)

valid = (
    enriched
    .filter(F.col("channel").isNotNull())
    .withColumn("quantity_adjusted_int", F.col("quantity_adjusted").cast("int"))
)

base = (
    valid
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") >= 340))
    .select(
        "*",
        F.round((F.col("running_avg_quantity") * 1.07), 2).alias("adjusted_running_avg_quantity"),
        F.round((F.col("running_count_quantity") * 1.15), 2).alias("running_count_quantity_adjusted"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    base
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity_adjusted",
        "running_min_quantity",
        "running_count_quantity",
        "running_avg_quantity",
        "running_sum_quantity",
        "shipping_fee",
        F.col("min_store_sale_units"),
    )
)
