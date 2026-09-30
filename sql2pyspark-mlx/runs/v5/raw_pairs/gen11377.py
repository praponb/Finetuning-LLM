from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5, 6]) & (F.col("revenue") >= 2900))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .orderBy("sale_cohort")
)
