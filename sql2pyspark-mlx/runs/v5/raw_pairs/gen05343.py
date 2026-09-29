from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-04-26").cast("date")) & (F.col("amount") > 1490))
    .select("*", F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("method") != "promptpay")
        .select("status")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.amount",
        "e.paid_at",
        "e.row_num",
        F.col("p.shipping_fee"),
        F.col("p.status").alias("order_status"),
        F.col("p.discount"),
    )
)

enriched = (
    with_dims
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.discount").alias("order_discount"))
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "order_status",
        "discount",
        "order_discount",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("avg_shipment_cost").desc_nulls_last())
)
