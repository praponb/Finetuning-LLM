from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("row_num").desc(), "id")

ranked = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .withColumn("prev_amount", F.lag("amount").over(w2))
    .withColumn("next_amount", F.lead("amount").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

base = (
    metrics
    .filter((F.col("txn_time") > F.lit("2023-09-18").cast("date")) & F.col("is_flagged"))
    .withColumn("seq_no_per_next_amount", F.round((F.col("seq_no") / F.col("next_amount")), 2))
    .withColumn("seq_no_filled", F.coalesce("seq_no", F.lit(0)))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("N/A")))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .select(
        "id",
        "txn_type",
        "amount",
        "row_num",
        "prev_amount",
        "next_amount",
        "seq_no",
        "seq_no_per_next_amount",
        "seq_no_filled",
    )
    .orderBy(F.col("txn_type").desc_nulls_first(), "id")
    .limit(5)
)
