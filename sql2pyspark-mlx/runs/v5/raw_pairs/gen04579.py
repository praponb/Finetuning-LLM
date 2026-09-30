from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy("account_type", "status").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.partitionBy("account_type")

cleaned = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

result = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("balance_dense_rank", F.dense_rank().over(w2))
    .withColumn("account_type_sum_balance", F.sum("balance").over(w3))
)
