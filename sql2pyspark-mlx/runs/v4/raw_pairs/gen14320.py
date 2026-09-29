from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount_filled").desc_nulls_first(), "id")

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2024-03-24 09:00:00").cast("timestamp"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
