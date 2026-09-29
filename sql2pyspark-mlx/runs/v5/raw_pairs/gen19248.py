from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([1, 2, 14, 32]))
    .select("*", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
)

result = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.category"), F.col("p.price"))
)
