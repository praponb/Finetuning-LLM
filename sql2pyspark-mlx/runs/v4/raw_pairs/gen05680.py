from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w_part = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first())

enriched = (
    df_transactions
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_5"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
        F.row_number().over(w3).alias("event_seq"),
        F.rank().over(w_part).alias("amount_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_type",
        "is_flagged",
        "amount_avg_5",
        "amount_percent_rank",
        "event_seq",
        "amount_rank",
        F.when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "EXTENDED").otherwise("PRIORITY").alias("txn_type_zone"),
    )
)
