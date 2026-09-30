from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.min("launch_date").alias("min_launch_date"),
    )
)
