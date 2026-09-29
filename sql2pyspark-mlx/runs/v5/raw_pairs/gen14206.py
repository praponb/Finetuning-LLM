from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", "s.account_type", "s.status", F.col("c.merchant"))
)

result = (
    latest_match
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") > 1)
    .orderBy("account_type")
)
