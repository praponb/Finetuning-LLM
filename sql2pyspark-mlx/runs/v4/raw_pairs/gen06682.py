from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "new_group_flag_tier", "order_id"], ["week_start", "new_group_flag_tier"], []],
        "week_start",
        "new_group_flag_tier",
        "order_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("paid_at").alias("last_paid_at"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .orderBy(F.col("order_id").asc_nulls_first())
)
