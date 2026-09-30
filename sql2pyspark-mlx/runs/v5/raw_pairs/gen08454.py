from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
