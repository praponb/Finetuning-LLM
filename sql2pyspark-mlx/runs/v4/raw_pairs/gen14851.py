from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("first_launch_date"))
    .orderBy("supplier_id", "first_launch_date")
    .limit(10)
)
