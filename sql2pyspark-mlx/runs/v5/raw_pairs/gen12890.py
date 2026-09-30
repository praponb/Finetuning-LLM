from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("store_id") == 2)

store_id_summary = (
    filtered_inventory
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 255, "high").when(F.col("quantity") >= 106, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    store_id_summary
    .filter(F.col("quantity_range") > 73)
    .orderBy(F.col("quantity_range").desc())
)
