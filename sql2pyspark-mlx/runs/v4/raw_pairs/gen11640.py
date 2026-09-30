from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.when(F.col("s.city").isin(["London", "Berlin"]), "Core").otherwise("Other").alias("city_group"),
    )
)
