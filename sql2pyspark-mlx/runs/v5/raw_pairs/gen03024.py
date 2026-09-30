from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "merchant")
w4 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-04-13 19:00:00").cast("timestamp"))
    .withColumn("txn_time_label", F.date_format("txn_time", "yyyy-MM-dd"))
)

result = (
    cleaned
    .filter(F.col("amount") < 3800)
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share_2"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
