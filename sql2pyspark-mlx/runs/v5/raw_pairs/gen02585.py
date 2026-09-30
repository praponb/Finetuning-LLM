from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("order_amount", "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("order_discount", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("order_discount").desc(), "id")

valid = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-06-01")
    .select(
        "id",
        "order_id",
        "status",
        F.date_add("delivered_date", 74).alias("delivered_date_plus_74d"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.order_date") >= "2024-01-11")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier_clean",
        F.col("b.discount"),
        F.col("b.amount"),
        F.col("b.order_date"),
    )
)

joined_shipments = (
    joined
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier_clean",
        "s.discount",
        "s.amount",
        F.col("p.discount").alias("order_discount"),
        F.col("p.amount").alias("order_amount"),
    )
)

scored = (
    joined_shipments
    .select(
        "id",
        "discount",
        "amount",
        "order_discount",
        F.ntile(3).over(w).alias("order_amount_bucket"),
        F.first("order_discount", ignorenulls=True).over(w2).alias("first_known_order_discount"),
        F.ntile(10).over(w3).alias("order_discount_bucket"),
        F.row_number().over(w4).alias("event_seq"),
        F.lag("amount", 1, 0).over(w3).alias("prev_amount"),
    )
)

base = (
    scored
    .filter(F.col("order_discount").between(0.16, 0.34))
    .select(
        "id",
        "discount",
        "amount",
        "order_discount",
        "order_amount_bucket",
        "first_known_order_discount",
        "order_discount_bucket",
        "prev_amount",
        F.floor("event_seq").alias("event_seq_whole"),
    )
)

report = (
    base
    .select(
        "id",
        "discount",
        "order_discount",
        "order_amount_bucket",
        "first_known_order_discount",
        "order_discount_bucket",
    )
)

result = (
    report
    .select("order_amount_bucket", "order_discount_bucket")
    .orderBy(F.col("order_discount_bucket").desc())
)
