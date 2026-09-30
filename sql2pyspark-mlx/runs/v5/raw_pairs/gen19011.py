from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)
