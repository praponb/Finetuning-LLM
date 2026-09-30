from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant")
w2 = Window.partitionBy("merchant").orderBy("days_since_txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-15 07:00:00").cast("timestamp")) & (F.col("merchant") == "Lazada"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.row_number().over(w2).alias("event_seq"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
    )
)
