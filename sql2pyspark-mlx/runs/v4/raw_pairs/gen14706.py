from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 2700) & F.col("store_id").isin([2, 4, 6]))
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy("sale_month")
)
