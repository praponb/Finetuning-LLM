from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 6]))
    .withColumn("least_revenue_units", F.least("revenue", "units"))
    .withColumn("greatest_revenue_units", F.greatest("revenue", "units"))
)

result = (
    filtered
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.stock_qty"), F.col("p.price"), F.col("p.launch_date"))
)
