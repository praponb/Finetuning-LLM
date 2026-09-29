from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1400, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
