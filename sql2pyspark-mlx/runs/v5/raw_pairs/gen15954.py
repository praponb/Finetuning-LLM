from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("method").orderBy("amount", "id")
w3 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_amount_per_rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_amount_per_rn", "id")
w6 = Window.orderBy("next_amount", "id")
w7 = Window.orderBy("last_known_running_amount_per_rn", "id")
w8 = Window.orderBy("prev_last_known_running_amount_per_rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("paid_at") > F.lit("2023-08-09").cast("date")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.concat_ws("/", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "paid_at",
        "rn",
        F.lead("amount", 1).over(w2).alias("next_amount"),
        F.sum("amount").over(w3).alias("running_amount"),
    )
)

base = (
    enriched
    .filter(F.col("paid_at") >= F.lit("2023-10-12").cast("date"))
    .select(
        "id",
        "next_amount",
        F.round((F.col("running_amount") / F.col("rn")), 2).alias("running_amount_per_rn"),
    )
)

scored = (
    base
    .select(
        "id",
        "next_amount",
        F.last("running_amount_per_rn", ignorenulls=True).over(w4).alias("last_known_running_amount_per_rn"),
        F.lead("next_amount", 2).over(w5).alias("next_next_amount"),
    )
)

enriched_payments = (
    scored
    .select(
        "id",
        "last_known_running_amount_per_rn",
        F.lead("next_amount").over(w6).alias("next_next_amount_2"),
        F.lag("next_amount").over(w6).alias("prev_next_amount"),
        F.lag("last_known_running_amount_per_rn").over(w6).alias("prev_last_known_running_amount_per_rn"),
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

enriched_payments_2 = (
    enriched_payments
    .select(
        "id",
        "last_known_running_amount_per_rn",
        "next_next_amount_2",
        "prev_last_known_running_amount_per_rn",
        "row_seq",
        F.lag("row_seq", 1, 0).over(w7).alias("prev_row_seq"),
        F.ntile(4).over(w7).alias("last_known_running_amount_per_rn_bucket"),
        F.sum("row_seq").over(w8).alias("running_row_seq"),
    )
)

result = (
    enriched_payments_2
    .withColumn("last_known_running_amount_per_rn_tier", F.when(F.col("last_known_running_amount_per_rn") >= 80, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_row_seq_tier", F.when(F.col("running_row_seq") >= 42, "LARGE").otherwise("SMALL"))
    .cube("last_known_running_amount_per_rn_tier", "running_row_seq_tier")
    .agg(
        F.avg("running_row_seq").alias("avg_running_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("row_seq").alias("max_row_seq"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("last_known_running_amount_per_rn"), 2).alias("avg_last_known_running_amount_per_rn"),
        F.percentile_approx("prev_row_seq", [0.25, 0.5, 0.75]).alias("prev_row_seq_quartiles"),
        F.grouping("last_known_running_amount_per_rn_tier").alias("grouping_level"),
    )
    .orderBy("avg_running_row_seq")
)
