from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("stddev_units") >= 52)
    .select("sale_month", "stddev_units", "avg_units")
)
