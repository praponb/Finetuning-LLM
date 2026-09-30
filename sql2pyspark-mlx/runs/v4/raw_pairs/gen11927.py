from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("txn_time").desc_nulls_last())

valid = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("txn_time_period", F.date_trunc("MONTH", "txn_time"))
    .withColumn("txn_type_merchant", F.concat("txn_type", F.lit(" - "), "merchant"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

result = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)
