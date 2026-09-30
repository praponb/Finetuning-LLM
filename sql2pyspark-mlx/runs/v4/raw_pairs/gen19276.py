from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(36, 38) & F.col("product_id").isin([5, 10]))
    .groupBy(
        F.when(F.col("units") >= 44, "high").when(F.col("units") >= 11, "mid").otherwise("low").alias("units_bucket"),
        "store_id",
    )
    .agg(F.countDistinct("units").alias("unique_stores"))
    .filter(F.col("unique_stores") < 2)
)
