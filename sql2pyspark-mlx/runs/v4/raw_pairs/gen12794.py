from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    latest_match
    .groupingSets(
        [["brand", "category", "supplier_id"], ["brand", "category"], ["brand"]],
        "brand",
        "category",
        "supplier_id",
    )
    .agg(
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.percentile_approx("inventory_quantity", 0.9).alias("inventory_quantity_p90"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
    )
)
