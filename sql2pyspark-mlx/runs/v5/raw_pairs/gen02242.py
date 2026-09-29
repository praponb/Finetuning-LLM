from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 6]) & (F.col("sale_date") <= "2024-03-07") & F.col("revenue").between(1400, 2900))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") < 47)
)
