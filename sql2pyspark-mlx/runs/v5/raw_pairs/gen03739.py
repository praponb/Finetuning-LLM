from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 167) & (F.col("last_restock") <= "2024-05-01"))
    .select(
        "last_restock",
        "product_id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
