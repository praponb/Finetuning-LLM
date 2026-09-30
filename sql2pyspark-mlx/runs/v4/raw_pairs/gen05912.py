from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(65, 181))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 259, "high").when(F.col("quantity") >= 78, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .orderBy("store_id")
)
