from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

not_flagged = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= F.col("s.order_date")), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isNotNull()), "left_anti")
)

metrics = (
    eligible
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "discount",
        F.avg("discount").over(w_part).alias("last7_avg_discount"),
        F.count("discount").over(w_part).alias("last7_count_discount"),
        F.sum("discount").over(w_part).alias("last7_sum_discount"),
        F.min("discount").over(w_part).alias("last7_min_discount"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"), F.col("c.delivered_date"))
)

cleaned = (
    with_last
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & (F.col("shipped_date") < "2023-12-01"))
    .select(
        "*",
        F.datediff("order_date", "delivered_date").alias("days_delivered_date_to_order_date"),
        (F.col("last7_avg_discount") + F.col("last7_count_discount")).alias("last7_avg_discount_plus_last7_count_discount"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.shipped_date")), "left_anti")
)

result = (
    kept
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.segment").alias("segment"), F.col("src.channel").alias("channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.last7_sum_discount")).alias("min_last7_sum_discount"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
