from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date_month_end").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("amount")
w3 = Window.partitionBy("channel", "order_id").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("order_id", "channel").orderBy(F.col("cost").asc_nulls_last())

filtered = (
    df_shipments
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-09-05")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "carrier", "shipped_date", "cost")
)

enriched = (
    ranked
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.id", "src.order_id", "src.cost", F.col("p.channel"))
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.channel") == "store")
    .select("src.*", F.col("p.amount"))
)

latest = (
    enriched_shipments
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "order_id",
        "cost",
        "channel",
        "amount",
        F.ntile(10).over(w3).alias("cost_bucket"),
        F.percent_rank().over(w4).alias("cost_percent_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("order_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "*",
        (F.col("amount") - F.col("cost_percent_rank")).alias("amount_minus_cost_percent_rank"),
    )
)

result = (
    cleaned
    .select("id", "channel", "amount", "cost")
    .unpivot(
        ids=["id", "channel"],
        values=["amount", "cost"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
