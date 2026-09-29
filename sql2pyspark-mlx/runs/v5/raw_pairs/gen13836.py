from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-04").cast("date")) & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .withColumn("merchant_upper", F.upper("merchant"))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .rollup("quarter_start", "txn_type")
    .agg(
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("amount_per_refund"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.grouping("quarter_start").alias("gid"),
    )
)
