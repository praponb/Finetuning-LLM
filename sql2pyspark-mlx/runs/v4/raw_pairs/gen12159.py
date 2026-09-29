from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("supplier_id").isin([1, 7, 38]))
    .withColumn("launch_date_period", F.date_trunc("week", "launch_date"))
)

agg = (
    recent_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = agg.filter(F.col("cost_range") < 21)
