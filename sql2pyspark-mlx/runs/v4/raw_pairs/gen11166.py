from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-08-24").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.round((F.col("cost") * 1.15), 2).alias("adjusted_cost"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.order_date"), F.col("b.status"), F.col("b.shipping_fee"))
)

joined_shipments = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.status").alias("order_status"))
)

enriched = (
    joined_shipments
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w2))
)

result = (
    enriched
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_rows_2"),
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
    )
    .filter(F.col("cancelled_rows_2") >= 1)
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
