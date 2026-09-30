from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("store_id", "status").orderBy("discount", "id")
w3 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("customer_id").orderBy("payment_paid_at", "id")
w6 = Window.partitionBy("customer_id").orderBy("payment_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.lower(F.col("p.store_name")).like("%ive%"))
    .select("src.*", F.col("p.store_name"), F.col("p.city"))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "rank_in_group",
        F.row_number().over(w2).alias("row_seq"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price") > 300), "left_semi")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < "2024-03-01"), "left_anti")
)

latest_match = (
    not_flagged_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.method").alias("payment_method"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

gaps = (
    latest_match
    .withColumn("is_new_burst", F.when(F.lag("payment_paid_at").over(w5).isNull() | (F.col("payment_paid_at") > F.lag("payment_paid_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w6))

result = (
    sessionized
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["segment", "status"], ["segment"], []],
        F.col("c.segment").alias("segment"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.row_seq")).alias("min_row_seq"),
        F.avg(F.col("a.is_new_burst")).alias("avg_is_new_burst"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("status").asc_nulls_first())
)
