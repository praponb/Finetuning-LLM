from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

valid = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-03-16").cast("date"))
    .select(
        "*",
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
        F.round("cost", 1).alias("cost_rounded"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
        F.ntile(10).over(w3).alias("cost_bucket"),
    )
)

cleaned = (
    metrics
    .filter((F.col("delivered_date") >= F.lit("2023-08-23").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("shipped_date_period", F.date_trunc("year", "shipped_date"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") >= 1430)
    .select(
        "a.id",
        "a.order_id",
        "a.delivered_date",
        "a.cost",
        "a.cumulative_cost",
        "a.cost_dense_rank",
        F.col("o.channel"),
        F.col("o.amount"),
    )
)

with_dims_shipments = (
    with_dims
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.cumulative_cost",
        "s.cost_dense_rank",
        "s.channel",
        "s.amount",
        F.col("o.discount"),
        F.col("o.amount").alias("order_amount"),
    )
)

enriched = (
    with_dims_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.cumulative_cost",
        "src.channel",
        "src.discount",
        "src.order_amount",
        F.col("d.amount").alias("order_amount_2"),
    )
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "channel",
        "discount",
        "order_amount",
        "order_amount_2",
        F.when(F.col("channel").rlike("^(partner|store)$"), "PRIORITY").otherwise("EXTENDED").alias("channel_zone"),
        F.col("min_payment_amount"),
    )
    .orderBy(F.col("min_payment_amount").desc())
)
