from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .orderBy(F.col("min_quantity").desc(), "product_id", "last_restock_month")
    .limit(20)
)
