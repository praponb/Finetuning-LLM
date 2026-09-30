from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("last_restock") < "2025-09-25")
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

agg = (
    recent_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.min("last_restock_month_end").alias("min_last_restock_month_end"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    agg
    .select("last_restock_year", "min_last_restock_month_end", "total_quantity")
    .orderBy(F.col("last_restock_year").desc())
)
