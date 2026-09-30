from pyspark.sql import functions as F

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.amount").alias("order_amount"), F.col("b.discount"))
)

result = (
    joined
    .groupBy(F.date_trunc("week", "paid_at").alias("week_start"), "method")
    .agg(
        F.percentile_approx("order_amount", 0.9).alias("order_amount_p90"),
        F.min("discount").alias("min_discount"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("discount")).otherwise(0)).alias("promptpay_discount"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
    )
)
