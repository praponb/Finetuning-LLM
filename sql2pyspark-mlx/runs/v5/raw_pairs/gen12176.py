from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w_ordered_2 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("last4_min_amount_whole", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-05-03 23:00:00"))) & F.col("order_id").isNotNull())
    .withColumn("amount_whole", F.ceil("amount"))
)

metrics = (
    filtered
    .select(
        "*",
        F.max("amount_whole").over(w_ordered).alias("last4_max_amount_whole"),
        F.min("amount_whole").over(w_ordered).alias("last4_min_amount_whole"),
        F.sum("amount_whole").over(w_ordered).alias("last4_sum_amount_whole"),
        F.sum("amount_whole").over(w_ordered_2).alias("amount_whole_sum_90d"),
    )
)

metrics_payments = (
    metrics
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "amount_whole_sum_90d",
        F.min("amount_whole").over(w3).alias("last7_min_amount_whole"),
        F.avg("amount_whole").over(w3).alias("last7_avg_amount_whole"),
        F.count("amount_whole").over(w3).alias("last7_count_amount_whole"),
    )
)

kept_keys = (
    metrics_payments
    .select("order_id")
    .intersect(
        df_payments
        .filter(F.to_date("paid_at") < "2023-03-16")
        .select("order_id")
    )
)

reconciled = (
    metrics_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.amount").alias("order_amount"), F.col("d.channel"))
)

result = (
    enriched
    .withColumn("amount_whole_sum_90d_tier", F.when(F.col("amount_whole_sum_90d") >= 316, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["amount_whole_sum_90d_tier", "order_id"], ["amount_whole_sum_90d_tier"], []],
        "amount_whole_sum_90d_tier",
        "order_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.countDistinct("method").alias("distinct_methods"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
