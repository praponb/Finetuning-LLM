from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .withColumn("amount_tier", F.when(F.col("amount") >= 3700, "PREMIUM").otherwise("STANDARD"))
    .cube("amount_tier")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.grouping("amount_tier").alias("grouping_level"),
    )
    .filter(F.col("debit_rows") < 1000)
)
