from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "amount",
        "order_id",
        "status",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
    )
)
