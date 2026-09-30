from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("merchant").orderBy(F.col("is_flagged_flag").desc())
w3 = Window.partitionBy("merchant").orderBy("is_flagged_flag", "id").rowsBetween(-5, -1)
w4 = Window.orderBy(F.col("amount").desc())

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-10-05").cast("date"))
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

result = (
    filtered
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "is_flagged",
        "merchant_filled",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.percent_rank().over(w2).alias("is_flagged_flag_percent_rank"),
        F.avg("is_flagged_flag").over(w3).alias("is_flagged_flag_avg_5_prior"),
        F.cume_dist().over(w4).alias("amount_cume_dist"),
    )
)
