from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-24 07:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("txn_type")
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.min("is_flagged_flag").alias("min_is_flagged_flag"),
    )
)
