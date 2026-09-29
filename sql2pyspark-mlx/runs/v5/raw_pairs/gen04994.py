from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1500, 3700) | (F.col("revenue") < 2500))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.sum("units").alias("total_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .orderBy(F.col("sale_month").desc())
)
