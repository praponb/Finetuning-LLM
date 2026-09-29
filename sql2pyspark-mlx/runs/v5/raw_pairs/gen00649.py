from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("merchant", "account_id")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
)

result = (
    cleaned
    .filter(F.col("account_id") == 2)
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_time_day",
        F.sum("amount").over(w).alias("amount_sum_7d"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share_2"),
    )
)
