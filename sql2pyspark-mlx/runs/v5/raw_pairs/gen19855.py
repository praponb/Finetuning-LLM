from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7]) & F.col("product_id").isin([8, 9]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy("store_id")
)
