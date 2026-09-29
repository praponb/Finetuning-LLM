from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
