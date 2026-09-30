from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("end_date", "id")
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w5 = Window.partitionBy("customer_id").orderBy("prev_monthly_fee_plus_seq_no", "id")
w6 = Window.partitionBy("status").orderBy("prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("first_known_monthly_fee_rank", "id")

ranked = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
        F.rank().over(w3).alias("monthly_fee_rank"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

cleaned = (
    metrics
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        "prev_monthly_fee",
        "monthly_fee_rank",
        "seq_no",
        (F.col("prev_monthly_fee") + F.col("seq_no")).alias("prev_monthly_fee_plus_seq_no"),
        F.add_months("end_date", 1).alias("end_date_plus_months"),
    )
)

metrics_subscriptions = (
    cleaned
    .select(
        "id",
        "monthly_fee",
        "status",
        "prev_monthly_fee_plus_seq_no",
        F.lead("prev_monthly_fee").over(w5).alias("next_prev_monthly_fee"),
        F.lag("prev_monthly_fee").over(w5).alias("prev_prev_monthly_fee"),
        F.lead("monthly_fee").over(w5).alias("next_monthly_fee"),
        F.row_number().over(w5).alias("row_seq"),
        F.first("monthly_fee_rank", ignorenulls=True).over(w6).alias("first_known_monthly_fee_rank"),
    )
)

with_windows = (
    metrics_subscriptions
    .filter(F.col("first_known_monthly_fee_rank").isNotNull())
    .select(
        "*",
        F.lead("monthly_fee").over(w7).alias("next_monthly_fee_2"),
        F.lag("prev_monthly_fee_plus_seq_no").over(w7).alias("prev_prev_monthly_fee_plus_seq_no"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

remaining_keys = (
    with_windows
    .select("status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id") == 40)
        .select("status")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "monthly_fee",
        "status",
        "next_prev_monthly_fee",
        "prev_prev_monthly_fee",
        "next_monthly_fee",
        "first_known_monthly_fee_rank",
        F.when(F.col("status").rlike("^(CANCELLED|ACTIVE)$"), "PRIORITY").when(F.col("status").rlike("^(PAUSED|EXPIRED)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy("prev_prev_monthly_fee", F.col("next_monthly_fee").desc())
)
