from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.count(F.when(F.col("p.brand") == "Globex", 1)).alias("globex_count"),
    )
    .filter(F.col("product_count") >= 1)
)
