from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")

metrics = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.avg("amount").over(w).alias("amount_avg_3_prior"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.count("amount").over(w3).alias("order_id_count_amount"),
    )
)

result = (
    metrics
    .withColumn("cumulative_amount_tier", F.when(F.col("cumulative_amount") >= 8728, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .groupingSets(
        [["cumulative_amount_tier", "week_start", "method"], ["cumulative_amount_tier", "week_start"], []],
        "cumulative_amount_tier",
        "week_start",
        "method",
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.min("paid_at").alias("started_at"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.countDistinct("method").alias("distinct_methods"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("distinct_methods").desc())
)
