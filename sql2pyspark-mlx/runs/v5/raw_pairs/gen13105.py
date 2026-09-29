from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")

cleaned = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
    )
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "is_flagged", "txn_type_upper")
)
