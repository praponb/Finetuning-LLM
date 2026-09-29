from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("running_count_cost").asc_nulls_first(), "id")
w_recent = Window.partitionBy("channel", "carrier").orderBy("shipped_date", "id")
w4 = Window.partitionBy("channel", "carrier").orderBy(F.col("running_count_cost").desc_nulls_first(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-09-15").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select("*", F.floor("cost").alias("cost_whole"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.channel"))
)

metrics = (
    with_dims
    .filter(F.col("order_id").isin([15, 25, 26, 31]))
    .select(
        "*",
        F.avg("cost").over(w_ordered).alias("running_avg_cost"),
        F.count("cost").over(w_ordered).alias("running_count_cost"),
        F.max("cost").over(w_ordered).alias("running_max_cost"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "channel",
        "running_count_cost",
    )
)

with_windows = (
    deduped
    .select(
        "*",
        F.lag("running_count_cost").over(w_recent).alias("prev_running_count_cost"),
        F.lead("running_count_cost").over(w_recent).alias("next_running_count_cost"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

filtered_shipments = (
    ranked
    .filter(F.col("order_id").isNotNull() & F.col("channel").isin(["store", "web"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "channel",
        "next_running_count_cost",
        "seq_no",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

result = (
    filtered_shipments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "channel",
        "next_running_count_cost",
        "seq_no",
        "days_since_shipped_date",
        "carrier_clean",
        F.col("min_payment_amount"),
    )
    .orderBy("id", F.col("next_running_count_cost").desc())
    .limit(5)
)
