from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount_whole", "id")
w2 = Window.partitionBy("txn_type", "merchant").orderBy("next_amount_whole")
w_part = Window.partitionBy("merchant").orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy(F.col("running_next_amount_whole").desc(), "id")
w5 = Window.partitionBy("txn_type").orderBy("next_amount_whole", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("running_next_amount_whole", "id")

filtered = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.floor("amount").alias("amount_whole"),
    )
)

with_windows = (
    filtered
    .filter(F.col("amount_whole") != 45)
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "amount_whole",
        F.lead("amount").over(w).alias("next_amount"),
        F.lead("amount_whole").over(w).alias("next_amount_whole"),
        F.row_number().over(w).alias("event_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "txn_type",
        "next_amount_whole",
        "event_seq",
        F.rank().over(w2).alias("next_amount_whole_rank"),
        F.sum("next_amount_whole").over(w_part).alias("running_next_amount_whole"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics_transactions = (
    deduped
    .withColumn("running_next_amount_whole_avg_6", F.avg("running_next_amount_whole").over(w5))
    .withColumn("next_next_amount_whole", F.lead("next_amount_whole", 1).over(w6))
)

filtered_transactions = (
    metrics_transactions
    .filter(F.col("event_seq").between(5, 6))
    .select(
        "id",
        "next_amount_whole",
        "event_seq",
        "running_next_amount_whole",
        "running_next_amount_whole_avg_6",
        "next_next_amount_whole",
        F.when(F.col("running_next_amount_whole") >= 439, "High").when(F.col("running_next_amount_whole") >= 89, "Medium").otherwise("Low").alias("running_next_amount_whole_band"),
    )
)

result = (
    filtered_transactions
    .select(
        "id",
        "next_amount_whole",
        "event_seq",
        "running_next_amount_whole_avg_6",
        "running_next_amount_whole_band",
    )
    .orderBy(F.col("running_next_amount_whole_avg_6").desc())
)
