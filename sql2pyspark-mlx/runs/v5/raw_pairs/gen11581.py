from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-19"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipment_status",
        F.col("c.status").alias("payment_status"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.cost").isNotNull()), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.first("shipment_status", ignorenulls=True).over(w4).alias("first_known_shipment_status"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupingSets(
        [["region", "city", "status"], ["region", "city"], ["region"], []],
        F.col("s.region").alias("region"),
        F.col("s.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.discount")).alias("min_discount"),
        F.max(F.col("a.first_known_amount")).alias("max_first_known_amount"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
