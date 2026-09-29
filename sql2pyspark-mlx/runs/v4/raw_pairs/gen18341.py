from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() | F.col("order_id").isNull())
    .select("id", "order_id", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)
