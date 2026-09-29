from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_transactions
    .withColumn("account_id_count_amount", F.count("amount").over(w))
    .withColumn("running_amount", F.sum("amount").over(w2))
    .withColumn("next_amount", F.lead("amount", 1).over(w3))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w4))
)

result = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
        F.avg(F.when(F.col("txn_type") == "DEBIT", F.col("account_id_count_amount"))).alias("debit_avg_account_id_count_amount"),
        F.min("txn_time").alias("first_seen"),
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
    )
)
