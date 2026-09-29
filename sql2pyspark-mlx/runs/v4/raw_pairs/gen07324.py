from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 230)
    .groupBy("category")
    .agg(
        F.count("*").alias("num_products"),
        F.max("launch_date").alias("last_launch_date"),
    )
    .filter(F.col("num_products") >= 2)
)
