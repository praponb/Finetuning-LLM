from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("quantity") != 67)
    .withColumn("last_restock_minus_28d", F.date_sub("last_restock", 28))
)

result = (
    filtered
    .select("id", "product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock").between("2023-07-06", "2024-07-01"))
        .select("id", "product_id")
    )
)
