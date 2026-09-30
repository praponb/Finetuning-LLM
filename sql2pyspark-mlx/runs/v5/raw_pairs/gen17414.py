from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)
