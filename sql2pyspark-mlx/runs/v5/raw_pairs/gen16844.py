from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("greatest_quantity_unit_price_filled").desc_nulls_last())
w_running = Window.partitionBy("product_id").orderBy("unit_price_whole", "id")
w3 = Window.orderBy("greatest_quantity_unit_price_filled", "id").rowsBetween(-3, -1)
w_running_2 = Window.orderBy("greatest_quantity_unit_price_filled", "id")
w_hist = Window.orderBy("greatest_quantity_unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_whole", F.floor("unit_price"))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.upper(F.trim(F.col("p.product_name"))) == "LAPTOP PRO 14")
    .select("s.*", F.col("p.launch_date"))
)

filtered = (
    with_dims
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_whole",
        "launch_date",
        F.coalesce("greatest_quantity_unit_price", F.lit(0)).alias("greatest_quantity_unit_price_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w).alias("greatest_quantity_unit_price_filled_cume_dist"),
        F.ntile(3).over(w_running).alias("unit_price_whole_bucket"),
        F.avg("unit_price_whole").over(w3).alias("unit_price_whole_avg_3_prior"),
        F.lead("unit_price_whole", 2).over(w_running_2).alias("next_unit_price_whole"),
        F.sum("quantity").over(w_hist).alias("running_quantity"),
    )
)

result = (
    enriched
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_whole",
        "greatest_quantity_unit_price_filled",
        "unit_price_whole_bucket",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("order_id").desc())
)
