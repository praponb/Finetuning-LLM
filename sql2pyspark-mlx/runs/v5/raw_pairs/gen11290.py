from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("shipped_date").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-06-04").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.floor("cost").alias("cost_whole"),
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.order_date").between("2023-06-07", "2025-04-13"))
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.cost",
        "a.status",
        "a.cost_whole",
        "a.shipped_date_plus_months",
        F.col("b.order_date"),
    )
)

joined_shipments = (
    joined
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.discount"), F.col("d.shipping_fee"))
)

enriched = (
    joined_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.cost",
        "s.status",
        "s.cost_whole",
        "s.discount",
        "s.shipping_fee",
        F.col("b.status").alias("order_status"),
        F.col("b.channel"),
        F.col("b.discount").alias("order_discount"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("order_discount").alias("ORDER_DISCOUNT_VAL"),
        "cost",
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["SHIPPING_FEE_VAL", "ORDER_DISCOUNT_VAL", "cost"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
