from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("adjusted_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("running_min_amount_filled", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy(F.col("running_sum_amount_filled").asc_nulls_first())

valid = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-06-21 14:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

filtered = (
    valid
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-09-22 13:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        "method_clean",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.round((F.col("amount") * 1.15), 2).alias("adjusted_amount"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.max("amount_filled").over(w).alias("running_max_amount_filled"),
        F.min("amount_filled").over(w).alias("running_min_amount_filled"),
        F.sum("amount_filled").over(w).alias("running_sum_amount_filled"),
        F.max("adjusted_amount").over(w2).alias("adjusted_amount_max_90d"),
    )
)

enriched = (
    scored
    .filter(F.col("amount") >= 720)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        "method_clean",
        "amount_filled",
        "running_max_amount_filled",
        "running_min_amount_filled",
        "running_sum_amount_filled",
        F.avg("running_max_amount_filled").over(w3).alias("running_max_amount_filled_avg_3"),
        F.sum("amount").over(w4).alias("running_amount"),
        F.percent_rank().over(w_grp).alias("running_sum_amount_filled_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "method_filled",
        "method_clean",
        "amount_filled",
        "running_max_amount_filled",
        "running_min_amount_filled",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "CORE").when(F.col("status").rlike("^(FAILED)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
    )
)
