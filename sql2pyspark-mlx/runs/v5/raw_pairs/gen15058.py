from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") >= "2025-02-20")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

result = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .filter(F.col("b.cost") > 280)
    .select(
        "e.id",
        "e.product_id",
        "e.days_since_last_restock",
        F.col("b.product_name"),
        F.col("b.cost"),
        F.col("b.launch_date"),
    )
)
