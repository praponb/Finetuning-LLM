from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand").isin(["Acme", "Initech", "Umbrella"]) | (F.col("s.country") != "SG"))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.city"),
        F.col("s.rating"),
    )
    .orderBy("rating", "id")
    .limit(20)
)
