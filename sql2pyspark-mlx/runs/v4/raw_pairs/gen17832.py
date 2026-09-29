from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "quantity",
        "store_id",
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
    )
)
