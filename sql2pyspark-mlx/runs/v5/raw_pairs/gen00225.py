from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first(), "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-04-20 04:00:00").cast("timestamp")) & (F.col("account_id") == 24))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
