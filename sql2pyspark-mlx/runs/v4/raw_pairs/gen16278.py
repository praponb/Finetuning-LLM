from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-10"))) & F.col("store_id").isin([2, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

result = (
    filtered
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([2, 3]))
        .select("store_id", "id")
    )
)
