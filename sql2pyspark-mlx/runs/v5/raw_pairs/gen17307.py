from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

result = (
    latest_match
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.max("balance").alias("max_balance"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("transaction_merchant")).alias("transaction_merchant_list"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
)
