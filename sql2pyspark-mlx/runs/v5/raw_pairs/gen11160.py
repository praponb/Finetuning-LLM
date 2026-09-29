from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-09-28"))) & F.col("amount").isNotNull())
    .select(
        "*",
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
        F.length("merchant").alias("merchant_length"),
    )
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("merchant_length", [0.25, 0.5, 0.75]).alias("merchant_length_quartiles"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)
