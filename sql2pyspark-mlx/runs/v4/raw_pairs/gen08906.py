from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(24, 34))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.max("units").alias("max_units"),
    )
    .orderBy("first_sale_date")
)
