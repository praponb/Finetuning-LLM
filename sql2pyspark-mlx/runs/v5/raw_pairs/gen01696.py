from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"), "brand")
    .agg(F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"))
    .orderBy("brand")
)
