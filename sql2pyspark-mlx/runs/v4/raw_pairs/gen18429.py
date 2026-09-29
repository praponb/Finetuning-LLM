from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(-5, -1)

enriched = (
    df_transactions
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.sum("amount").over(w_hist).alias("cumulative_amount"),
        F.avg("amount").over(w3).alias("amount_avg_5_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "first_known_amount",
        "amount_avg_5_prior",
        F.when(F.col("merchant").rlike("^(Amazon|Lazada)$"), "CORE").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "EXTENDED").otherwise("GROUP_A").alias("merchant_zone"),
    )
    .orderBy(F.col("amount_avg_5_prior").desc())
)
