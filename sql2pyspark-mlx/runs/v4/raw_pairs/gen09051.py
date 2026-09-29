from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("status", "customer_id")
w3 = Window.partitionBy("account_type").orderBy("balance")
w4 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.when(F.col("balance") >= 63000, "High").when(F.col("balance") >= 34000, "Medium").otherwise("Low").alias("balance_tier"),
    )
)

result = (
    base
    .withColumn("balance_dense_rank", F.dense_rank().over(w))
    .withColumn("status_sum_balance", F.sum("balance").over(w2))
    .withColumn("balance_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("cumulative_balance", F.sum("balance").over(w4))
)
