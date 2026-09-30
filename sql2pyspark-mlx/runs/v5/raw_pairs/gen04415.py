from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("order_status", "order_id").orderBy(F.col("order_date").desc(), "id")
w5 = Window.partitionBy("order_status")
w6 = Window.partitionBy("order_status").orderBy(F.col("order_date").desc(), "id")

base = (
    df_shipments
    .filter((F.col("carrier").eqNullSafe(F.lit("UPS"))) & F.col("status").isin(["DELIVERED", "LOST"]))
    .withColumn("carrier_group", F.when(F.col("carrier").isin(["UPS", "DHL"]), "Priority").otherwise("Other"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("carrier", ignorenulls=True).over(w2).alias("last_known_carrier"),
        F.ntile(4).over(w3).alias("cost_bucket"),
    )
)

base_shipments = (
    enriched
    .filter((F.col("shipped_date") > F.lit("2023-03-02").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "carrier_group",
        "status_title",
        "carrier_filled",
        "seq_no",
        "last_known_carrier",
        "cost_bucket",
        F.upper("carrier").alias("carrier_upper"),
    )
)

base_shipments_2 = (
    base_shipments
    .filter(F.col("order_id") == 23)
    .withColumn("cost_band", F.when(F.col("cost") >= 105, "High").when(F.col("cost") >= 15, "Medium").otherwise("Low"))
    .withColumn("seq_no_scaled", F.round((F.col("seq_no") * 0.85), 2))
)

with_dims = (
    base_shipments_2
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.shipping_fee").between(15, 30))
    .select(
        "s.id",
        "s.order_id",
        "s.carrier_group",
        "s.status_title",
        "s.seq_no",
        "s.last_known_carrier",
        "s.cost_bucket",
        "s.carrier_upper",
        "s.cost_band",
        "s.seq_no_scaled",
        F.col("b.amount"),
        F.col("b.status").alias("order_status"),
        F.col("b.order_date"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "carrier_group",
        "last_known_carrier",
        "carrier_upper",
        "amount",
        "order_status",
        "order_date",
        F.row_number().over(w4).alias("row_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "last_known_carrier",
        "amount",
        "order_date",
        "row_seq",
        "rank_in_group",
    )
)
