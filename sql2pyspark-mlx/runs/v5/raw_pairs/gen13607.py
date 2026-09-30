from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([6, 26, 33, 38]) & ((F.col("amount") >= 1370) | (F.col("order_date") >= "2024-02-01")))
    .select(
        "id",
        "customer_id",
        "discount",
        F.month("order_date").alias("order_date_month"),
    )
)
