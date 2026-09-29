from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.month("last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.max("last_restock").alias("last_last_restock"))
)
