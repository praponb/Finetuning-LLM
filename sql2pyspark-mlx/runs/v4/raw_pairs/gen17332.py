from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.max(F.col("p.cost")).alias("max_cost"),
        F.count(F.when(F.col("p.brand") == "Stark", 1)).alias("stark_count"),
    )
)
