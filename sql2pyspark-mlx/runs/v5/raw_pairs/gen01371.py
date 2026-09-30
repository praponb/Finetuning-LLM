from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4, 5]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("revenue_count") >= 3)
    .orderBy(F.col("last_sale_date").desc(), F.col("avg_units").desc(), "sale_cohort")
    .limit(1)
)
