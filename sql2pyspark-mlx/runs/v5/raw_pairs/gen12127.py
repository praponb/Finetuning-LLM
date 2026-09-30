from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-01-01")
    .withColumn("sale_date_year", F.year("sale_date"))
)

store_sales_stats = (
    valid_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("stddev_units") < 72)
    .select("sale_month", "min_sale_date")
    .orderBy("sale_month")
)
