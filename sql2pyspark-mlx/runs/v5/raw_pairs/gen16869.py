from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter((F.col("revenue") < 3900) | F.col("units").isNotNull())
    .withColumn("sale_date_period", F.date_trunc("month", "sale_date"))
)

store_sales_stats = (
    valid_store_sales
    .groupBy(
        F.when(F.year("sale_date_period") < 2024, "before_2024").when(F.year("sale_date_period") == 2024, "2024").otherwise("after_2024").alias("sale_period_cohort"),
    )
    .agg(F.max("units").alias("max_units"))
)

result = (
    store_sales_stats
    .filter(F.col("max_units") > 28)
    .orderBy(F.col("sale_period_cohort").desc())
)
