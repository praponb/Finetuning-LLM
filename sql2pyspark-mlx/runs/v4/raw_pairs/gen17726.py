from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .groupingSets(
        [["activity_month", "merchant"], ["activity_month"], []],
        "activity_month",
        "merchant",
    )
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.grouping("activity_month").alias("grouping_level"),
    )
)
