from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount")
w2 = Window.partitionBy("store_id", "status").orderBy("amount_with_shipping", "id")
w3 = Window.partitionBy("status", "channel").orderBy(F.col("rn").desc())
w_running = Window.partitionBy("customer_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-31"))) & F.col("store_id").isNotNull())
    .withColumn("adjusted_amount", F.round((F.col("amount") * 100), 2))
    .withColumn("amount_with_shipping", (F.col("amount") * F.col("shipping_fee")))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("rn_2", F.rank().over(w3))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

enriched = (
    ranked
    .withColumn("running_sum_discount", F.sum("discount").over(w_running))
    .withColumn("running_avg_discount", F.avg("discount").over(w_running))
    .withColumn("running_count_discount", F.count("discount").over(w_running))
    .withColumn("running_max_discount", F.max("discount").over(w_running))
)

ranked_orders = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "customer_id",
        "channel_clean",
        "latest_rank",
        "rn",
        "running_avg_discount",
        "running_max_discount",
    )
)

eligible = (
    ranked_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price").between(110, 150)), "left_semi")
)

result = (
    eligible
    .select("id", "customer_id", "running_max_discount", "running_avg_discount")
    .unpivot(
        ids=["id", "customer_id"],
        values=["running_max_discount", "running_avg_discount"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
