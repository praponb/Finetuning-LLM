from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]) & ((F.col("sale_date") <= "2025-08-01") | F.col("units").between(11, 13)))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("sale_cohort")
)
