from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first(), "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-23").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("txn_time_hour", F.hour("txn_time"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
