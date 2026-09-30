from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(1)
)
