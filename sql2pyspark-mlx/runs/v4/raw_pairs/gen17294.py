from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("shipping_fee") >= 44, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("returned_count") >= 1)
    .orderBy(F.col("distinct_status").desc_nulls_first())
)
