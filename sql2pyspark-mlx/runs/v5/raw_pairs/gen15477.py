from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("units").between(13, 23) | F.col("revenue").between(1100, 2700)))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.count("*").alias("num_store_sales"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy("num_store_sales")
)
