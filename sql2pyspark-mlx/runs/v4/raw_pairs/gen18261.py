from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").desc(), "id")
w_running = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w5 = Window.partitionBy("status", "order_id").orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2024-02-02").cast("date"))
    .withColumn("status_carrier", F.concat("status", F.lit(" "), "carrier"))
)

with_windows = (
    base
    .withColumn("running_cost", F.sum("cost").over(w_part))
    .withColumn("cost_bucket", F.ntile(5).over(w2))
    .withColumn("order_id_min_cost", F.min("cost").over(w_running))
)

with_windows_shipments = (
    with_windows
    .select(
        "*",
        F.lead("cost_bucket", 2).over(w4).alias("next_cost_bucket"),
        F.sum("cost_bucket").over(w5).alias("cumulative_cost_bucket"),
        F.round((F.col("cost") / F.sum("cost").over(w_running)), 4).alias("cost_share"),
        F.sum("order_id_min_cost").over(w6).alias("running_order_id_min_cost"),
    )
)

with_dims = (
    with_windows_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.shipping_fee"), F.col("b.amount"))
)

with_dims_shipments = (
    with_dims
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.shipping_fee").alias("order_shipping_fee"),
        F.col("d.status").alias("order_status"),
    )
)

base_shipments = (
    with_dims_shipments
    .filter(F.col("delivered_date") > F.lit("2024-03-24").cast("date"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status_carrier",
        "running_cost",
        "cost_bucket",
        "order_id_min_cost",
        "cumulative_cost_bucket",
        "cost_share",
        "order_shipping_fee",
        "order_status",
        F.col("cumulative_cost_bucket").cast("string").alias("cumulative_cost_bucket_str"),
        F.coalesce("next_cost_bucket", F.lit(0)).alias("next_cost_bucket_filled"),
    )
)

result = (
    base_shipments
    .select(
        "id",
        "order_id",
        "status_carrier",
        "cost_bucket",
        "order_shipping_fee",
        "order_status",
        "next_cost_bucket_filled",
        F.when(F.col("order_status").rlike("^(PENDING|RETURNED)$"), "GROUP_A").when(F.col("order_status").rlike("^(COMPLETED|CANCELLED)$"), "OTHER_ZONE").otherwise("CORE").alias("order_status_zone"),
    )
    .orderBy(F.col("order_status").desc_nulls_last(), "order_status_zone")
)
