from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select(
        "quantity",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
