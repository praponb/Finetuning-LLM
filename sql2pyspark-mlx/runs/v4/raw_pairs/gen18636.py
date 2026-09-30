from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_last())
w_part = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("promo_code").orderBy("shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("order_date", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("amount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id")
w8 = Window.partitionBy("status").orderBy(F.col("amount_rank").asc_nulls_last(), "id", "promo_id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

not_flagged = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") < F.col("s.order_date")), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("discount_avg_5", F.avg("discount").over(w_hist))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("seq_no", F.row_number().over(w_part))
)

attributed = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics_orders = (
    attributed
    .withColumn("first_known_discount_pct", F.first("discount_pct", ignorenulls=True).over(w4))
    .withColumn("running_discount_avg_5", F.sum("discount_avg_5").over(w_running))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
)

deduped = (
    metrics_orders
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "discount_avg_5",
        F.col("min_order_discount"),
    )
)
