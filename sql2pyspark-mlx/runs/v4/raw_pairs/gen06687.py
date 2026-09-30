from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 8]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "store_id",
    )
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .filter(F.col("stddev_units") >= 47)
    .select("sale_cohort", "store_id", "stddev_revenue", "stddev_units")
)
