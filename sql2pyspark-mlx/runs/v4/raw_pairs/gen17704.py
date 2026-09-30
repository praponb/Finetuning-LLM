from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-07-01"))) & F.col("order_id").isNotNull() & (F.col("method") == "bank_transfer"))
    .select(
        "*",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)

result = (
    base
    .select("order_id")
    .subtract(
        df_payments
        .filter(F.col("amount") < 700)
        .select("order_id")
    )
)
