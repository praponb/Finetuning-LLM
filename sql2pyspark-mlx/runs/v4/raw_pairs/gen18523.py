from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.orderBy("txn_time", "id").rowsBetween(-5, -1)
w_recent_2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc())

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-05-10 14:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

result = (
    cleaned
    .withColumn("prev_amount", F.lag("amount", 1, 0).over(w))
    .withColumn("amount_avg_5", F.avg("amount").over(w2))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent_2))
    .withColumn("amount_rank", F.rank().over(w5))
)
