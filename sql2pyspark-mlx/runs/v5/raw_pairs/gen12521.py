from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("shipping_fee").desc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "shipping_fee",
        F.add_months("order_date", 1).alias("order_date_plus_months"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "customer_id",
        "status",
        "shipping_fee",
        "order_date_plus_months",
        F.lead("shipping_fee", 2).over(w).alias("next_shipping_fee"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_3"),
        F.dense_rank().over(w3).alias("shipping_fee_dense_rank"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") >= F.col("s.order_date_plus_months")), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date_plus_months")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.shipped_date"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date").alias("shipment_shipped_date"))
)

grouped = (
    latest_match
    .groupBy(
        F.when(F.col("shipping_fee_avg_3") >= 34, "HIGH").otherwise("LOW").alias("shipping_fee_avg_3_tier"),
        F.date_trunc("QUARTER", "order_date_plus_months").alias("quarter_start"),
        "status",
    )
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("shipping_fee_per_pending"),
        F.round(F.avg("shipping_fee_avg_3"), 2).alias("avg_shipping_fee_avg_3"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
    )
)

result = (
    grouped
    .select(
        "shipping_fee_avg_3_tier",
        "quarter_start",
        "status",
        "shipped_rows",
        "pending_rows",
        "avg_shipping_fee_avg_3",
        "cancelled_rows",
        "shipping_fee_quartiles",
    )
    .orderBy(F.col("shipping_fee_avg_3_tier").desc())
)
