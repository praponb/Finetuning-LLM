from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("store_id").isin([1, 3, 4]))

agg = (
    recent_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"), "store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    agg
    .select("sale_month", "store_id", "revenue_range")
    .orderBy(F.col("sale_month").desc(), "store_id")
    .limit(5)
)
