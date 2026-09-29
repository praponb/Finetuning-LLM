from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 107)
    .select(
        "id",
        "store_id",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label_2"),
    )
)
