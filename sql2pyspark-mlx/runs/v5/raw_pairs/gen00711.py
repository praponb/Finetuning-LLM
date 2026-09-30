from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 199, "high").when(F.col("quantity") >= 61, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy(F.col("quantity_bucket").desc(), "first_last_restock")
)
