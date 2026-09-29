from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Fashion")
    .groupBy("supplier_id", "category")
    .agg(F.countDistinct("price").alias("unique_suppliers"))
    .filter(F.col("unique_suppliers") >= 4)
    .select("supplier_id", "category", "unique_suppliers")
    .orderBy("category", "supplier_id")
    .limit(3)
)
