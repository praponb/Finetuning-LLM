from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        "product_id",
        F.when(F.col("quantity") >= 203, "high").when(F.col("quantity") >= 67, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 4)
    .drop("row_count")
    .orderBy(F.col("quantity_bucket").desc())
)
