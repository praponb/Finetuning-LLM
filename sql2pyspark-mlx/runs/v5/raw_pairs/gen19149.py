from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("amount", "id")

base = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-06-27").cast("date"))
    .select("*", F.upper("txn_type").alias("txn_type_upper"))
)

result = (
    base
    .filter(F.col("account_id") == 39)
    .withColumn("amount_bucket", F.ntile(3).over(w))
    .withColumn("amount_sum_90d", F.sum("amount").over(w2))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w3))
    .withColumn("amount_bucket_2", F.ntile(10).over(w_running))
    .withColumn("amount_bucket_3", F.ntile(5).over(w_running))
)
