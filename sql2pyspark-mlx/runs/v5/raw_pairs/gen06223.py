from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(223, 389) & F.array_contains("tags", "eco") & F.col("brand").isin(["Initech", "Stark"]))
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .orderBy("launch_month")
)
