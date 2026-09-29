from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")
w2 = Window.orderBy(F.col("discount").desc(), "id")
w3 = Window.orderBy("shipping_fee_int", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy(F.col("amount").desc(), "id")
w_ordered = Window.partitionBy("store_id").orderBy("shipping_fee_int", "id")
w_grp = Window.partitionBy("store_id")
w7 = Window.partitionBy("channel").orderBy("order_date", "id")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-02-23").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price") <= 310), "left_anti")
)

latest = (
    not_flagged
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.avg("amount").over(w3).alias("amount_avg_6"),
        F.ntile(5).over(w4).alias("amount_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("discount").over(w_grp).alias("store_id_sum_discount"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "amount_bucket",
    )
)

with_dims = (
    ranked
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.amount",
        "a.discount",
        "a.amount_bucket",
        F.col("b.age"),
        F.col("b.email"),
        F.col("b.credit_limit"),
    )
)

not_flagged_orders = (
    with_dims
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 5), "left_anti")
)

result = (
    not_flagged_orders
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.segment").alias("segment"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.amount_bucket")).alias("max_amount_bucket"),
        F.min(F.col("src.amount")).alias("min_amount"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
