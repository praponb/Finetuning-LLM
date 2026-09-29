from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "product_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
