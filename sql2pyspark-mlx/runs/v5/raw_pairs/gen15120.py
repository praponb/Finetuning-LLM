from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status")
w_recent = Window.orderBy(F.col("order_date").desc(), "id")
w3 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w4 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

with_dims = (
    df_orders
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.region") == "North")
    .select(
        "a.id",
        "a.store_id",
        "a.order_date",
        "a.status",
        "a.shipping_fee",
        F.col("d.store_name"),
        F.col("d.opened_date"),
        F.col("d.sqft"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        "store_name",
        "sqft",
        F.max("sqft").over(w_grp).alias("status_max_sqft"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.ntile(10).over(w3).alias("shipping_fee_bucket"),
        F.first("sqft", ignorenulls=True).over(w4).alias("first_known_sqft"),
        F.avg("sqft").over(w5).alias("sqft_avg_6_prior"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"), F.col("c.shipped_date"))
)

enriched = (
    latest_match
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.store_name",
        "s.status_max_sqft",
        "s.event_seq",
        "s.shipping_fee_bucket",
        "s.first_known_sqft",
        "s.sqft_avg_6_prior",
        "s.carrier",
        "s.shipped_date",
        F.col("b.region"),
    )
)

result = (
    enriched
    .groupBy("region")
    .pivot("carrier", ["DHL", "UPS"])
    .agg(F.sum("status_max_sqft").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "region",
        F.col("DHL_value_sum").alias("dhl_value_sum"),
        F.col("DHL_n").alias("dhl_n"),
        F.col("UPS_value_sum").alias("ups_value_sum"),
        F.col("UPS_n").alias("ups_n"),
    )
)
