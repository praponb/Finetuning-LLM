from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("least_unit_price_quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("event_seq", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.launch_date"), F.col("d.stock_qty"))
)

scored = (
    joined
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "launch_date",
        "stock_qty",
        F.lead("line_total").over(w).alias("next_line_total"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("event_seq"),
    )
)

metrics = (
    scored
    .withColumn("prev_stock_qty", F.lag("stock_qty", 2, 0).over(w2))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w3))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w4))
)

result = (
    metrics
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "launch_date",
        "stock_qty",
        "next_line_total",
        "next_unit_price",
        "prev_unit_price",
        "prev_stock_qty",
        F.col("avg_store_sale_units"),
    )
    .orderBy("launch_date")
)
