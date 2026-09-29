from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("burst_id_tier").orderBy(F.col("avg_is_new_burst_whole").desc())

filtered = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.substring("method", 1, 2).alias("method_prefix"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

valid = (
    sessionized
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-11-20 11:00:00"))) & (F.col("is_new_burst") >= 0))
    .select("*", F.ceil("is_new_burst").alias("is_new_burst_whole"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.when(F.col("is_new_burst_whole") >= 62, "LARGE").otherwise("SMALL").alias("is_new_burst_whole_tier"),
        F.date_trunc("week", "paid_at").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.round(F.avg("is_new_burst_whole"), 2).alias("avg_is_new_burst_whole"),
    )
    .withColumn("avg_is_new_burst_whole_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("burst_id_tier").desc_nulls_first())
)
