from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5, 6]) | F.col("product_id").isin([2, 3, 10]))
)

store_sales_stats = (
    recent_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("units").alias("units_count"),
    )
)

result = store_sales_stats.orderBy(F.col("units_count").desc(), F.col("units_range").desc())
