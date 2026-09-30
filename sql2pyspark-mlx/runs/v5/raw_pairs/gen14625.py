from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .filter(~F.col("merchant").isin(["Shell", "Starbucks", "Lazada"]))
    .select(
        "id",
        "merchant",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.count("amount").over(w2).alias("amount_count_90d"),
    )
)
