from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([3, 11]))
    .select(
        "id",
        "amount",
        "method",
        F.concat("status", F.lit(", "), "method").alias("status_method"),
    )
)
