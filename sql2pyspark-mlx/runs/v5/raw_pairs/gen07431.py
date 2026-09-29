from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("units").isNull() | (F.col("units") > 19))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 6))
)

agg = (
    src
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    agg
    .select("store_id", "revenue_range")
    .orderBy("store_id", F.col("revenue_range").desc())
)
