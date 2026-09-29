from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .agg(F.sum("cost").alias("total_cost"))
)
