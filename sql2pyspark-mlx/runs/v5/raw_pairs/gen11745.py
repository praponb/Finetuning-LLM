from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("merchant", "txn_type").orderBy("amount_int")
w3 = Window.partitionBy("account_id").orderBy("amount_int", "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("amount") >= 1500))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    valid
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "amount_int",
        "merchant_filled",
        F.rank().over(w).alias("amount_rank"),
        F.dense_rank().over(w2).alias("amount_int_dense_rank"),
        F.lag("amount", 1, 0).over(w3).alias("prev_amount"),
    )
)
