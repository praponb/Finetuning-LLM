from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2024-01-01") | (F.col("product_id") == 3))
    .select(
        "id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy("quantity", F.col("last_restock_month_end").desc())
)
