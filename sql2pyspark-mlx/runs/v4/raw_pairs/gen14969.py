from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("method").orderBy("amount", "id")
w_part = Window.partitionBy("order_id").orderBy("amount")
w_ordered = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("order_id").orderBy(F.col("amount_share").desc())
w_grp_2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy("cumulative_amount_rounded", "id")
w9 = Window.partitionBy("status").orderBy(F.col("amount_share_cume_dist").asc_nulls_first())
w10 = Window.partitionBy("method").orderBy("amount_percent_rank", "id")

scored = (
    df_payments
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
    )
)

metrics = (
    scored
    .withColumn("amount_percent_rank", F.percent_rank().over(w_part))
    .withColumn("running_amount", F.sum("amount").over(w_ordered))
    .withColumn("amount_share_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w_grp_2))
    .withColumn("cumulative_amount", F.sum("amount").over(w7))
)

filtered = (
    metrics
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-08-14")))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "prev_amount",
        "amount_percent_rank",
        "amount_share_cume_dist",
        "last_known_method",
        F.round("cumulative_amount", 0).alias("cumulative_amount_rounded"),
    )
)

metrics_payments = (
    filtered
    .withColumn("next_amount_percent_rank", F.lead("amount_percent_rank", 2).over(w8))
    .withColumn("amount_share_cume_dist_percent_rank", F.percent_rank().over(w9))
    .withColumn("row_seq", F.row_number().over(w10))
)

matched_keys = (
    metrics_payments
    .select("status", "method")
    .intersectAll(
        df_payments
        .filter(F.to_date("paid_at") < "2024-10-26")
        .select("status", "method")
    )
)

kept_rows = (
    metrics_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.status").alias("order_status"))
)

result = (
    with_dims
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "prev_amount",
        "amount_share_cume_dist",
        "last_known_method",
        "cumulative_amount_rounded",
        "next_amount_percent_rank",
        F.when(F.col("method").rlike("^(bank_transfer|promptpay)$"), "CORE").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_B").otherwise("GROUP_A").alias("method_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
