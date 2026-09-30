from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]) | F.col("quantity").between(182, 205))
    .select(
        "id",
        "last_restock",
        "product_id",
        "quantity",
        F.col("quantity").cast("double").alias("quantity_str"),
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
    .orderBy("quantity", F.col("id").desc())
)
