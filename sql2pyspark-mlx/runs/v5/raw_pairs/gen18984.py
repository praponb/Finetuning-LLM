from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("status", "txn_type").orderBy(F.col("running_amount").asc_nulls_first(), "id")

cleaned = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Grab"))) & (F.col("amount") > 2300))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.customer_id").isNotNull())
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.merchant",
        "e.is_flagged_label",
        "e.running_amount",
        "e.amount_dense_rank",
        F.col("b.balance"),
    )
)

with_dims_transactions = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status"), F.col("b.account_type"))
)

latest = (
    with_dims_transactions
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged_label",
        "amount_dense_rank",
        "balance",
        "status",
        "account_type",
        "latest_rank",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
