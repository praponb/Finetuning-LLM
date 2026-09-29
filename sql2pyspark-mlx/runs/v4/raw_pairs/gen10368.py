from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("order_id", "id")
    .subtract(
        df_payments
        .filter(F.col("method") == "cash")
        .select("order_id", "id")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("amount") >= 1640, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_rows"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
    )
    .orderBy(F.col("card_rows").asc_nulls_last())
)
