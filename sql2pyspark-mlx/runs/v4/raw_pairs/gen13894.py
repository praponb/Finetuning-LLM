from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 2)
    .select(
        "id",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("id").desc())
)
