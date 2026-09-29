from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-02-02", "2024-09-01"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)
