from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").asc_nulls_last())
w_running = Window.orderBy("line_total_filled", "id")
w3 = Window.partitionBy("order_id").orderBy("line_total", "id")
w_part = Window.partitionBy("order_id").orderBy(F.col("line_total_filled").desc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc_nulls_first())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([10, 37]))
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.amount").between(900, 1330))
    .select("a.*", F.col("p.shipping_fee"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.status"))
)

cleaned = (
    with_dims
    .filter(F.col("row_num") <= 1)
    .select(
        "id",
        "order_id",
        "quantity",
        "line_total",
        "row_num",
        "shipping_fee",
        F.coalesce("line_total", F.lit(0)).alias("line_total_filled"),
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "order_id",
        F.ntile(10).over(w_running).alias("line_total_filled_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.rank().over(w_part).alias("line_total_filled_rank"),
        F.dense_rank().over(w5).alias("shipping_fee_dense_rank"),
        F.ntile(10).over(w_running).alias("line_total_filled_bucket_2"),
    )
)

daily = (
    with_windows
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("line_total_filled_bucket_2", [0.25, 0.5, 0.75]).alias("line_total_filled_bucket_2_quartiles"),
        F.percentile_approx("line_total_filled_bucket", 0.9).alias("line_total_filled_bucket_p90"),
    )
)

result = (
    daily
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "num_rows",
        "line_total_filled_bucket_2_quartiles",
        "line_total_filled_bucket_p90",
        F.col("avg_shipment_cost"),
    )
)
