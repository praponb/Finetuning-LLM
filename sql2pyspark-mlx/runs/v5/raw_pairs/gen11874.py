from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~F.col("p.brand").isin(["Stark", "Acme", "Umbrella"]) | F.col("s.supplier_name").like("%Siam%"))
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("p.category"),
    )
)
