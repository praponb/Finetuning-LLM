from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("Yog%") & (F.col("launch_date") > "2024-03-01"))
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
    )
    .orderBy(F.col("sports_count").desc())
)
