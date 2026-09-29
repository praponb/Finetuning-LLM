from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 18, "mid").otherwise("low").alias("units_bucket"),
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("min_revenue") > 2600)
    .orderBy("min_revenue")
)
