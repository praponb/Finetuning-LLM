from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-10-26").cast("date")) & F.col("store_id").isNotNull() & F.col("status").isin(["PENDING", "RETURNED", "COMPLETED"]))
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("unassigned")))
)

result = (
    base
    .select("id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isin([7, 14, 26, 32]))
        .select("id", "customer_id")
    )
)
