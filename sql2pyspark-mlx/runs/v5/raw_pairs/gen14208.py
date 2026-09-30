from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(360, 370))
    .select(
        F.lit("order_item").alias("record_type"),
        F.col("unit_price").alias("value"),
    )
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").between("2023-09-01", "2025-08-04"))
        .select(
        F.lit("subscription").alias("record_type"),
        F.col("monthly_fee").alias("value"),
    )
    )
    .distinct()
)
