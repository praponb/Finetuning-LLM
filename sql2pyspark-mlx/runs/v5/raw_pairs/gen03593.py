from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("amount") > 650) & (F.to_date("paid_at") < "2023-05-02"))
    .groupBy("status", "method")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
    )
    .filter(F.col("promptpay_amount") > 56)
    .orderBy("promptpay_amount", "method")
)
