from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id")
w4 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("account_id").orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
        F.first("merchant", ignorenulls=True).over(w2).alias("first_known_merchant"),
    )
)

enriched = (
    with_windows
    .filter(F.col("prev_amount") >= 1000)
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w4))
    .withColumn("running_prev_amount", F.sum("prev_amount").over(w5))
)

kept_keys = (
    enriched
    .select("account_id", "id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type") != "FEE")
        .select("account_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("merchant").eqNullSafe(F.lit("Grab"))) & (F.col("amount") < 2100))
    .select(
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        "prev_amount",
        "event_seq",
        "running_prev_amount",
        F.when(F.col("prev_amount") >= 4400, "High").when(F.col("prev_amount") >= 700, "Medium").otherwise("Low").alias("prev_amount_band"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("merchant").isNotNull())
    .select(
        "amount",
        "merchant",
        "is_flagged",
        "prev_amount",
        "prev_amount_band",
        "merchant_filled",
        (F.col("running_prev_amount") * F.col("event_seq")).alias("running_prev_amount_times_event_seq"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled_2"),
    )
)

report = (
    valid
    .select(
        "amount",
        "merchant",
        "prev_amount",
        "prev_amount_band",
        "merchant_filled",
        "running_prev_amount_times_event_seq",
        "merchant_filled_2",
    )
    .orderBy(F.col("merchant").desc_nulls_first())
)

result = (
    report
    .select(
        "amount",
        "merchant",
        "prev_amount",
        "prev_amount_band",
        "merchant_filled",
        F.when(F.col("merchant").rlike("^(Grab|Starbucks)$"), "GROUP_A").otherwise("GROUP_B").alias("merchant_zone"),
    )
)
