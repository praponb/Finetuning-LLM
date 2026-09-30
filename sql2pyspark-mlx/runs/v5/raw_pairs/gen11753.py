from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel")
w2 = Window.partitionBy("customer_id", "store_id")
w3 = Window.partitionBy("store_id", "status")
w4 = Window.partitionBy("status")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") >= F.lit("2023-10-07").cast("date")))
    .withColumn("order_date_label", F.date_format("order_date", "yyyy-MM-dd"))
)

remaining_keys = (
    filtered
    .select("id", "status")
    .subtract(
        df_orders
        .filter(F.col("order_date") < "2023-12-23")
        .select("id", "status")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("corr_amount_discount", F.corr("amount", "discount").over(w_part))
    .withColumn("corr_amount_shipping_fee", F.corr("amount", "shipping_fee").over(w2))
)

metrics = (
    with_windows
    .select(
        "customer_id",
        "status",
        "amount",
        "discount",
        "order_date_label",
        "corr_amount_discount",
        F.corr("amount", "shipping_fee").over(w3).alias("corr_amount_shipping_fee_2"),
    )
)

enriched = (
    metrics
    .select(
        "customer_id",
        "status",
        "order_date_label",
        "corr_amount_discount",
        "corr_amount_shipping_fee_2",
        F.avg("corr_amount_discount").over(w4).alias("status_avg_corr_amount_discount"),
    )
)

stats = (
    enriched
    .groupBy(
        "customer_id",
        F.when(F.col("corr_amount_shipping_fee_2") >= 0.68, "PREMIUM").otherwise("STANDARD").alias("corr_amount_shipping_fee_2_tier"),
    )
    .agg(
        F.round(F.avg("status_avg_corr_amount_discount"), 2).alias("avg_status_avg_corr_amount_discount"),
        F.percentile_approx("corr_amount_discount", [0.25, 0.5, 0.75]).alias("corr_amount_discount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("avg_status_avg_corr_amount_discount") > 0)
)

result = (
    stats
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "corr_amount_shipping_fee_2_tier",
        "num_status",
        F.col("sum_account_balance"),
    )
)
