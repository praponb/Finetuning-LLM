from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("order_discount").desc(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("order_discount").desc_nulls_last(), "id")

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.discount"), F.col("p.status").alias("order_status"))
)

enriched_payments = (
    enriched
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.status",
        "a.discount",
        F.col("o.order_date"),
        F.col("o.discount").alias("order_discount"),
        F.col("o.amount").alias("order_amount"),
    )
)

filtered = (
    enriched_payments
    .filter(F.col("discount") < 0.29)
    .select(
        "*",
        F.col("order_amount").cast("int").alias("order_amount_int"),
        F.last_day("order_date").alias("order_date_month_end"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("order_discount") >= 0.35, "HIGH").otherwise("LOW").alias("order_discount_tier"),
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("order_discount", [0.25, 0.5, 0.75]).alias("order_discount_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("order_amount_int")).otherwise(0)).alias("success_order_amount_int"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("success_order_amount_int").asc_nulls_last())
)
