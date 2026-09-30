from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([4, 7, 11]) & (F.col("units") < 25))
    .cube(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .orderBy("sale_month", "product_id")
)
