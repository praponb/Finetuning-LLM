from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("merchant")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") > F.lit("2023-07-02 12:00:00").cast("timestamp")))
    .select("*", F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"))
)

result = (
    filtered
    .filter(F.col("account_id") == 31)
    .select(
        "id",
        "txn_time_label",
        F.rank().over(w).alias("amount_rank"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.avg("amount").over(w3).alias("amount_avg_5_prior"),
        F.sum("amount").over(w4).alias("amount_sum_30d"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
    )
)
