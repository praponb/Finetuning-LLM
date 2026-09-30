from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_flagged_flag").desc(), "id")
w2 = Window.partitionBy("txn_type", "merchant").orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("amount", "id")
w4 = Window.orderBy("amount_bucket", "id")
w5 = Window.orderBy("balance", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-02 07:00:00").cast("timestamp")) & F.col("amount").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

enriched = (
    filtered
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.account_type").isin(["CREDIT", "LOAN", "CHECKING"]))
    .select("e.*", F.col("a.balance"))
)

enriched_transactions = (
    enriched
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "is_flagged_flag",
        "is_flagged_label",
        "balance",
        F.ntile(3).over(w).alias("is_flagged_flag_bucket"),
        F.sum("is_flagged_flag").over(w2).alias("running_is_flagged_flag"),
        F.ntile(3).over(w3).alias("amount_bucket"),
    )
)

enriched_transactions_2 = (
    enriched_transactions
    .select(
        "id",
        "txn_time",
        "balance",
        F.lead("amount_bucket").over(w4).alias("next_amount_bucket"),
        F.lag("amount_bucket").over(w4).alias("prev_amount_bucket"),
        F.lag("is_flagged_flag_bucket").over(w4).alias("prev_is_flagged_flag_bucket"),
        F.lead("is_flagged_flag_bucket").over(w4).alias("next_is_flagged_flag_bucket"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("is_flagged_flag_bucket").over(w5).alias("is_flagged_flag_bucket_avg_3"),
    )
)

result = (
    enriched_transactions_2
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("next_amount_bucket") >= 8, "LARGE").otherwise("SMALL").alias("next_amount_bucket_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("txn_time").alias("first_txn_time"),
        F.round(F.avg("prev_is_flagged_flag_bucket"), 2).alias("avg_prev_is_flagged_flag_bucket"),
        F.percentile_approx("is_flagged_flag_bucket_avg_3", [0.25, 0.5, 0.75]).alias("is_flagged_flag_bucket_avg_3_quartiles"),
    )
    .filter(F.col("avg_prev_is_flagged_flag_bucket") > 0)
)
