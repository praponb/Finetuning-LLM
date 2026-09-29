from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 7, 9]))
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 42, "high").when(F.col("units") >= 6, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.countDistinct("revenue").alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
