from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-29"))) & F.col("store_id").isin([3, 6]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

result = (
    base
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        "product_id",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("activity_month").desc())
)
