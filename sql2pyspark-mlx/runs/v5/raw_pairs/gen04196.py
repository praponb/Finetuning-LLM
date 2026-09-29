from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())
w2 = Window.partitionBy("method").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.discount"),
        F.col("b.status").alias("order_status"),
        F.col("b.order_date"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "method",
        "amount",
        "status",
        "discount",
        "order_status",
        "order_date",
    )
)

filtered = (
    deduped
    .filter(F.col("amount").between(610, 720))
    .select(
        "id",
        "method",
        "discount",
        "order_status",
        "order_date",
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("method") == "card")
    .withColumn("last3_avg_discount", F.avg("discount").over(w2))
    .withColumn("last3_max_discount", F.max("discount").over(w2))
    .withColumn("last3_sum_discount", F.sum("discount").over(w2))
)

totals = (
    metrics
    .groupBy(F.date_trunc("year", "order_date").alias("year_start"))
    .agg(
        F.sum("last3_avg_discount").alias("total_last3_avg_discount"),
        F.sum(F.when(F.col("order_status") == "SHIPPED", F.col("last3_sum_discount")).otherwise(0)).alias("shipped_last3_sum_discount"),
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("total_last3_avg_discount") > 0)
)

result = (
    totals
    .select(
        "year_start",
        "total_last3_avg_discount",
        "shipped_last3_sum_discount",
        "num_methods",
        "event_count",
    )
    .orderBy(F.col("shipped_last3_sum_discount").desc())
)
