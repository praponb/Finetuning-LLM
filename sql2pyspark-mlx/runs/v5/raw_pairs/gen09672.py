from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter((F.col("store_id") == 5) & F.col("units").between(11, 15))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    summary
    .select("store_id", "min_sale_date")
    .orderBy(F.col("min_sale_date").desc(), F.col("store_id").desc())
    .limit(5)
)
