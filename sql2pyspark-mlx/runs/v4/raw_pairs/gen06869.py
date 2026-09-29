from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .withColumn("amount_tier", F.when(F.col("amount") >= 3300, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("year", "txn_time"))
    .groupingSets(
        [["amount_tier", "year_start"], ["amount_tier"], []],
        "amount_tier",
        "year_start",
    )
    .agg(
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("merchant") == "Starbucks", 1)), F.lit(0)), 2).alias("is_new_session_per_starbucks"),
    )
    .orderBy(F.col("amount_tier").desc_nulls_first())
)
