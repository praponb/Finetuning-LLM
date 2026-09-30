from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "store_id",
        "quantity",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period_2"),
    )
    .orderBy(F.col("last_restock_period_2").desc())
)
