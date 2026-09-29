from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-22").cast("date")) & (F.col("store_id") == 6))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        F.when(F.col("channel") == "partner", 1).otherwise(0).alias("is_partner"),
    )
)

result = (
    cleaned
    .select("channel")
    .subtract(
        df_orders
        .filter(F.col("order_date") > "2024-03-01")
        .select("channel")
    )
)
