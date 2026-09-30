from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .groupingSets(
        [["session_no", "merchant"], ["session_no"], []],
        "session_no",
        "merchant",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("merchant") == "Lazada", F.col("is_new_burst"))).alias("lazada_avg_is_new_burst"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("merchant").desc())
)
