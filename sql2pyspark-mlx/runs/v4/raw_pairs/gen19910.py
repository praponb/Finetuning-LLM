from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity_plus_greatest_quantity_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("stock_qty", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity_plus_greatest_quantity_unit_price").desc(), "id")
w6 = Window.partitionBy("order_id")
w_recent = Window.partitionBy("product_id").orderBy("next_greatest_quantity_unit_price", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("order_id", "product_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id", "order_id").orderBy(F.col("first_known_quantity_filled").desc(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

filtered = (
    df_order_items
    .filter(F.col("unit_price") < 260)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base = (
    latest
    .filter(F.col("order_id").isin([2, 18, 30]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "greatest_quantity_unit_price",
        (F.col("quantity") + F.col("greatest_quantity_unit_price")).alias("quantity_plus_greatest_quantity_unit_price"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.stock_qty"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.first("quantity_filled", ignorenulls=True).over(w2).alias("first_known_quantity_filled"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.lead("greatest_quantity_unit_price", 1).over(w4).alias("next_greatest_quantity_unit_price"),
        F.ntile(5).over(w5).alias("quantity_plus_greatest_quantity_unit_price_bucket"),
        F.count("quantity").over(w6).alias("order_id_count_quantity"),
    )
)

enriched_order_items = (
    with_windows
    .withColumn("last_known_quantity_avg_6", F.avg("last_known_quantity").over(w_recent))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w8))
    .withColumn("first_known_quantity_filled_bucket", F.ntile(5).over(w9))
)

result = (
    enriched_order_items
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "greatest_quantity_unit_price",
        "stock_qty",
        "first_known_quantity_filled",
        "last_known_quantity",
        "next_greatest_quantity_unit_price",
        "order_id_count_quantity",
        F.col("min_store_sale_units"),
    )
    .orderBy(F.col("min_store_sale_units").desc())
)
