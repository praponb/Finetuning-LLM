from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("brand") == "Acme") & (F.col("launch_date") > "2024-04-22") & (F.col("launch_date") <= "2025-10-14"))
)

summary = (
    base
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
    )
)

result = summary.orderBy("supplier_id")
