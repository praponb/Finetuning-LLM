from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("revenue") < 3100)
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)

agg = (
    recent_store_sales
    .groupBy("product_id")
    .agg(F.max("revenue_int").alias("max_revenue_int"))
)

result = (
    agg
    .filter(F.col("max_revenue_int") < 29)
    .select("product_id", "max_revenue_int")
    .orderBy(F.col("product_id").desc())
)
