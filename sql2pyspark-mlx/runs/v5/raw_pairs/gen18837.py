from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("is_bank_transfer", "id")
w2 = Window.partitionBy("method").orderBy("is_bank_transfer", "id")
w3 = Window.partitionBy("method").orderBy(F.col("next_is_bank_transfer").desc(), "id")
w4 = Window.partitionBy("method").orderBy("is_bank_transfer", "id").rowsBetween(-5, -1)
w5 = Window.orderBy(F.col("is_bank_transfer_bucket").desc_nulls_last())
w6 = Window.partitionBy("order_id", "status").orderBy("is_bank_transfer_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method", "status").orderBy("next_is_bank_transfer", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("paypal"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.when(F.col("method") == "bank_transfer", 1).otherwise(0).alias("is_bank_transfer"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w_hist).alias("is_bank_transfer_bucket"),
        F.lead("is_bank_transfer", 1).over(w2).alias("next_is_bank_transfer"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    ranked
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "resolved_method",
        "next_is_bank_transfer",
        F.avg("is_bank_transfer_bucket").over(w4).alias("is_bank_transfer_bucket_avg_5_prior"),
        F.percent_rank().over(w5).alias("is_bank_transfer_bucket_percent_rank"),
        F.sum("is_bank_transfer").over(w6).alias("running_is_bank_transfer"),
        F.first("is_bank_transfer", ignorenulls=True).over(w7).alias("first_known_is_bank_transfer"),
    )
)

summary = (
    with_windows
    .groupBy("order_id")
    .agg(
        F.percentile_approx("first_known_is_bank_transfer", [0.25, 0.5, 0.75]).alias("first_known_is_bank_transfer_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.percentile_approx("next_is_bank_transfer", [0.25, 0.5, 0.75]).alias("next_is_bank_transfer_quartiles"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
    )
)

result = (
    summary
    .select(
        "order_id",
        "first_known_is_bank_transfer_quartiles",
        "refunded_count",
        "next_is_bank_transfer_quartiles",
        "paypal_rows",
    )
)
