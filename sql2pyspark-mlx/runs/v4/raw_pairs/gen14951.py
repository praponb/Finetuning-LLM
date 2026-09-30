from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 7, 8]) & F.col("store_id").isin([2, 4, 5]))
)

store_sales_stats = (
    filtered_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.count("units").alias("units_count"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("units_count") > 9)
    .select("sale_cohort", "units_count", "revenue_range")
    .orderBy("sale_cohort")
    .limit(50)
)
