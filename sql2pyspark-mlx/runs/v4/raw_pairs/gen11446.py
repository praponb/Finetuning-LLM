from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

summary = (
    valid_inventory
    .groupBy(F.year("last_restock_month_end").alias("last_restock_month_end_year"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = summary.select("last_restock_month_end_year", "avg_quantity")
