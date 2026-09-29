from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 8) & F.col("product_id").isin([4, 9, 10]))
    .select(
        "id",
        "product_id",
        "store_id",
        F.date_add("last_restock", 61).alias("last_restock_plus_61d"),
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)
