from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "New York")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("s.rating"),
        F.col("s.country"),
        F.greatest(F.col("p.cost"), F.col("p.price")).alias("greatest_cost_price"),
    )
)
