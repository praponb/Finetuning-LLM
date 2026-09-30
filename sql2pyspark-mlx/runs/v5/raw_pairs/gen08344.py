from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("cost").desc())
w2 = Window.partitionBy("carrier").orderBy("amount", "id")
w_running = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount", "id")

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.shipping_fee").between(11, 31))
    .select(
        "s.*",
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
        F.col("o.amount"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("cost_cume_dist"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("discount", ignorenulls=True).over(w_running).alias("last_known_discount"),
        F.lag("discount", 1).over(w4).alias("prev_discount"),
    )
)

joined_shipments = (
    scored
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.shipped_date",
        "s.delivered_date",
        "s.cost",
        "s.status",
        "s.discount",
        "s.amount",
        "s.cost_cume_dist",
        "s.seq_no",
        "s.prev_discount",
        F.col("d.channel"),
        F.col("d.status").alias("order_status_2"),
    )
)

base = (
    joined_shipments
    .filter(F.col("shipped_date") > F.lit("2023-05-18").cast("date"))
    .withColumn("seq_no_plus_discount", (F.col("seq_no") + F.col("discount")))
    .withColumn("discount_minus_prev_discount", (F.col("discount") - F.col("prev_discount")))
    .withColumn("order_status_2_filled", F.coalesce("order_status_2", F.lit("DIRECT")))
)

result = (
    base
    .select("id", "status", "seq_no_plus_discount", "prev_discount", "discount")
    .unpivot(
        ids=["id", "status"],
        values=["seq_no_plus_discount", "prev_discount", "discount"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
