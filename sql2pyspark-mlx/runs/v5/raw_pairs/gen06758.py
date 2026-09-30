from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.18, 0.21))
    .select(
        "id",
        "channel",
        "order_date",
        "status",
        F.upper("status").alias("status_upper"),
    )
)
