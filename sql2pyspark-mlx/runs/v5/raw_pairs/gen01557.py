from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 6, 10]) & (F.col("revenue") > 3300))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") < 54)
    .orderBy("sale_month")
)
