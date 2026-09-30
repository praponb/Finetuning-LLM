from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
