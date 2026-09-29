from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("order_status", "status").orderBy("cost_avg_6_rounded", "id")
w4 = Window.partitionBy("order_status").orderBy("cost_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.month("shipped_date").isin([6, 10, 12]))
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.upper("carrier").alias("carrier_upper"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.delivered_date",
        "src.cost",
        "src.status",
        "src.status_lower",
        "src.carrier_upper",
        F.col("o.amount"),
        F.col("o.status").alias("order_status"),
    )
)

enriched = (
    with_dims
    .withColumn("cost_avg_6", F.avg("cost").over(w_ordered))
    .withColumn("amount_bucket", F.ntile(4).over(w2))
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.discount") <= 0.16)
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost",
        "src.status",
        "src.amount",
        "src.order_status",
        "src.cost_avg_6",
        F.col("b.discount"),
        F.col("b.shipping_fee"),
    )
)

valid = (
    enriched_shipments
    .filter(F.col("cost_avg_6").between(41, 46))
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "order_status",
        "cost_avg_6",
        "discount",
        "shipping_fee",
        F.round("cost_avg_6", 2).alias("cost_avg_6_rounded"),
        F.lower("status").alias("status_lower"),
    )
)

enriched_shipments_2 = (
    valid
    .select(
        "id",
        "cost_avg_6",
        "discount",
        "shipping_fee",
        "status_lower",
        F.row_number().over(w3).alias("seq_no"),
        F.first("cost", ignorenulls=True).over(w4).alias("first_known_cost"),
    )
)

result = (
    enriched_shipments_2
    .select("id", "cost_avg_6", "first_known_cost")
    .orderBy(F.col("cost_avg_6").asc_nulls_last())
)
