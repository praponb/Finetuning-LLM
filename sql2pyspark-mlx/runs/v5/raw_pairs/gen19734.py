from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("account_id").orderBy("adjusted_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.orderBy("adjusted_amount", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant").eqNullSafe(F.lit("Starbucks"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round((F.col("amount") * 0.9), 2).alias("adjusted_amount"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

result = (
    base
    .select(
        "id",
        "account_id",
        "txn_type",
        "is_flagged",
        "adjusted_amount",
        "resolved_merchant",
        F.avg("amount").over(w).alias("amount_avg_2_prior"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
        F.rank().over(w3).alias("amount_rank"),
        F.avg("adjusted_amount").over(w4).alias("adjusted_amount_avg_2"),
    )
)
