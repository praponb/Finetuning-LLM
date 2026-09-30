from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-28").cast("date")) & (F.col("txn_type") == "DEBIT"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("OTHER")))
)

result = (
    base
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w2))
)
