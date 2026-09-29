from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.max("sale_date").alias("latest_sale_date"),
        F.count("*").alias("row_count"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("avg_units") >= 34)
    .drop("avg_units")
    .orderBy("latest_sale_date")
)
