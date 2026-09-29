from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

valid = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-07-05")))
    .withColumn("price_int", F.col("price").cast("int"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "launch_date",
        "tags",
        "supplier_id",
        F.sum("stock_qty").over(w2).alias("running_sum_stock_qty"),
        F.min("stock_qty").over(w2).alias("running_min_stock_qty"),
        F.max("stock_qty").over(w2).alias("running_max_stock_qty"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

result = (
    with_latest
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "launch_date",
        "tags",
        "supplier_id",
        "running_sum_stock_qty",
        "running_min_stock_qty",
        F.col("max_product_cost"),
    )
    .orderBy(F.col("max_product_cost").asc_nulls_last(), "running_sum_stock_qty")
)
