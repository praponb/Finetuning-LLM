from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount_rounded", "id")
w_recent = Window.orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("prev_amount_adjusted", "id")
w4 = Window.orderBy("running_count_next_amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
        F.round("amount", 0).alias("amount_rounded"),
    )
)

enriched = (
    base
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time_label",
        "amount_rounded",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lag("amount_rounded").over(w).alias("prev_amount_rounded"),
        F.lead("amount_rounded").over(w).alias("next_amount_rounded"),
        F.row_number().over(w).alias("event_seq"),
    )
)

cleaned = (
    enriched
    .filter(F.col("prev_amount").between(2200, 2300))
    .select(
        "*",
        F.round((F.col("prev_amount_rounded") / F.col("amount")), 2).alias("prev_amount_rounded_per_amount"),
        F.round((F.col("prev_amount") * 0.9), 2).alias("prev_amount_adjusted"),
    )
)

filtered = (
    cleaned
    .filter(F.col("prev_amount_adjusted") < 63)
    .withColumn("greatest_amount_prev_amount_rounded", F.greatest("amount", "prev_amount_rounded"))
)

cleaned_transactions = (
    filtered
    .filter(F.col("prev_amount_adjusted") > 21)
    .select(
        "id",
        "amount",
        "amount_rounded",
        "prev_amount",
        "next_amount_rounded",
        "prev_amount_adjusted",
        "greatest_amount_prev_amount_rounded",
        (F.col("prev_amount_rounded") * F.col("prev_amount_rounded_per_amount")).alias("prev_amount_rounded_times_prev_amount_rounded_per_amount"),
        F.trim("txn_type").alias("clean_txn_type"),
    )
)

enriched_transactions = (
    cleaned_transactions
    .filter(F.col("greatest_amount_prev_amount_rounded") > 38)
    .withColumn("running_max_next_amount_rounded", F.max("next_amount_rounded").over(w_recent))
    .withColumn("running_min_next_amount_rounded", F.min("next_amount_rounded").over(w_recent))
    .withColumn("running_count_next_amount_rounded", F.count("next_amount_rounded").over(w_recent))
)

enriched_transactions_2 = (
    enriched_transactions
    .withColumn("next_prev_amount_rounded_times_prev_amount_rounded_per_amount", F.lead("prev_amount_rounded_times_prev_amount_rounded_per_amount").over(w_hist))
    .withColumn("next_amount", F.lead("amount").over(w_hist))
    .withColumn("prev_amount_2", F.lag("amount").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("first_known_prev_amount", F.first("prev_amount", ignorenulls=True).over(w4))
)

result = (
    enriched_transactions_2
    .groupBy(
        F.when(F.col("running_min_next_amount_rounded") >= 61, "HIGH").otherwise("LOW").alias("running_min_next_amount_rounded_tier"),
        F.when(F.col("amount") >= 4100, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("prev_amount_rounded_times_prev_amount_rounded_per_amount") >= 68, "PREMIUM").otherwise("STANDARD").alias("prev_amount_rounded_times_prev_amount_rounded_per_amount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("row_seq").alias("max_row_seq"),
        F.sum("running_count_next_amount_rounded").alias("total_running_count_next_amount_rounded"),
    )
)
