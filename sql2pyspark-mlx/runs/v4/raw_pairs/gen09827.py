from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity_rounded").desc(), "id")
w_grp = Window.partitionBy("order_id").orderBy(F.col("adjusted_quantity").desc(), "id")
w3 = Window.partitionBy("product_id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") <= 5))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

valid = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

cleaned_order_items = (
    valid
    .filter(F.col("product_id").isNotNull() & (F.col("line_total") > 49))
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        "adjusted_quantity",
        "quantity_rounded",
        F.round((F.col("adjusted_quantity") * 0.9), 2).alias("adjusted_quantity_adjusted"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

enriched = (
    cleaned_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.stock_qty"), F.col("b.brand"))
)

result = (
    enriched
    .withColumn("quantity_rounded_bucket", F.ntile(3).over(w))
    .withColumn("adjusted_quantity_bucket", F.ntile(3).over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("product_id_sum_stock_qty", F.sum("stock_qty").over(w3))
)
