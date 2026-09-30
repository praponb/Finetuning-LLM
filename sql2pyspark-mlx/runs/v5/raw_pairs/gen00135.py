from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 181)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
    .orderBy(F.col("last_restock").desc(), "quantity_rounded", "id")
    .limit(10)
)
