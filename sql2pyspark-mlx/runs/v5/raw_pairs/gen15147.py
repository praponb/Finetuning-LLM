from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1000)
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .filter(F.col("min_revenue") >= 1200)
    .orderBy("min_revenue", "sale_month")
    .limit(20)
)
