from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([5, 16, 17, 21]))
    .groupBy("supplier_id", "category")
    .agg(F.max("cost").alias("max_cost"))
    .orderBy("category", "supplier_id")
    .limit(5)
)
