from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("status_length_minus_amount").desc(), "id")
w_recent = Window.orderBy("status_length_minus_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("status_length_minus_amount", "id")
w_grp = Window.orderBy("status_length_minus_amount", "id")
w5 = Window.partitionBy("status").orderBy(F.col("event_seq").asc_nulls_last(), "id")

base = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.length("status").alias("status_length"),
    )
)

valid = (
    base
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "status",
        (F.col("status_length") - F.col("amount")).alias("status_length_minus_amount"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    ranked
    .filter(F.col("status") == "REFUNDED")
    .select(
        "id",
        "status",
        F.sum("status_length_minus_amount").over(w_recent).alias("cumulative_status_length_minus_amount"),
        F.row_number().over(w3).alias("event_seq"),
        F.lead("status_length_minus_amount", 2, 0).over(w_grp).alias("next_status_length_minus_amount"),
    )
)

ranked_payments = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

stats = (
    ranked_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("next_status_length_minus_amount")).otherwise(0)).alias("failed_next_status_length_minus_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select(
        "status",
        "failed_next_status_length_minus_amount",
        "distinct_ids",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").asc_nulls_first())
)
