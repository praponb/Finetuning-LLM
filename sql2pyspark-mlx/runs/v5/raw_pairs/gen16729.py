from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("quantity").between(210, 212))
    .withColumn("last_restock_minus_3d", F.date_sub("last_restock", 3))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

result = (
    valid
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "region"], ["city"]],
        F.col("d.city").alias("city"),
        F.col("d.region").alias("region"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.days_since_last_restock")), 2).alias("total_days_since_last_restock"),
    )
)
