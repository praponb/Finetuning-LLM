from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

scored = (
    df_orders
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.sum("discount").over(w_ordered).alias("cumulative_discount"),
        F.ntile(4).over(w2).alias("discount_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    ranked
    .filter(F.col("channel").isNotNull() & (F.col("order_date") > F.to_date(F.lit("2023-06-10"))))
    .select("*", F.trim("channel").alias("clean_channel"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.amount",
        "s.discount",
        "s.discount_bucket",
        "s.row_seq",
        "s.rank_in_group",
        "s.clean_channel",
        F.col("c.status").alias("payment_status"),
    )
)

base = (
    latest_match
    .filter(F.col("payment_status").isNotNull() & (F.col("discount") <= 0.27))
    .withColumn("least_amount_discount", F.least("amount", "discount"))
    .withColumn("rank_in_group_plus_discount", (F.col("rank_in_group") + F.col("discount")))
    .withColumn("payment_status_clean", F.coalesce("payment_status", F.lit("OTHER")))
)

remaining_keys = (
    base
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("amount") > 460)
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .groupBy(
        F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.avg(F.when(F.col("payment_status") == "SUCCESS", F.col("amount"))).alias("success_avg_amount"),
        F.countDistinct("payment_status").alias("distinct_payment_status"),
        F.max(F.when(F.col("payment_status") == "REFUNDED", F.col("row_seq"))).alias("refunded_max_row_seq"),
        F.max(F.when(F.col("payment_status") == "SUCCESS", F.col("discount_bucket"))).alias("success_max_discount_bucket"),
    )
    .filter(F.col("success_max_discount_bucket") >= 0)
)

result = (
    daily
    .select("row_seq_tier", "success_avg_amount", "distinct_payment_status")
    .orderBy("row_seq_tier")
)
