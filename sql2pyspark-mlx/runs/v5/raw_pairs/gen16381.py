from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy(F.col("store_id").desc())
)
