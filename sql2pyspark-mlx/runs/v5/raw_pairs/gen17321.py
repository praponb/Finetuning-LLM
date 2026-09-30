from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status", "order_id").orderBy("amount_rounded", "id")
w2 = Window.partitionBy("status").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-01").cast("date")) & F.col("method").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 2))
    .withColumn("paid_at_date", F.to_date("paid_at"))
)

metrics = (
    filtered
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_rounded",
        F.lead("amount_rounded").over(w_recent).alias("next_amount_rounded"),
        F.lag("amount_rounded").over(w_recent).alias("prev_amount_rounded"),
        F.lead("amount").over(w_recent).alias("next_amount"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy("status")
    .agg(
        F.avg(F.when(F.col("method") == "cash", F.col("prev_amount_rounded"))).alias("cash_avg_prev_amount_rounded"),
        F.round(F.sum("next_amount_rounded") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("next_amount_rounded_per_success"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
