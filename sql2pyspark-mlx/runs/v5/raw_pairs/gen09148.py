from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_channel").orderBy(F.col("amount").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-02-24"))) & F.col("order_id").isNull())
    .select("*", F.trim("carrier").alias("clean_carrier"))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "shipped_date", "cost")
)

with_dims = (
    deduped
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.id", "a.order_id", F.col("p.status"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.id", "s.order_id", F.col("d.channel"))
)

with_dims_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        F.col("b.discount"),
        F.col("b.amount"),
        F.col("b.channel").alias("order_channel"),
    )
)

enriched_shipments = (
    with_dims_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.amount").alias("order_amount"), F.col("o.channel"))
)

with_windows = (
    enriched_shipments
    .select(
        "id",
        "order_id",
        "discount",
        "amount",
        "order_channel",
        "channel",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.sum("order_amount").over(w3).alias("cumulative_order_amount"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.ntile(5).over(w4).alias("amount_bucket_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ids=["id", "channel"],
        values=["DISCOUNT_VAL", "AMOUNT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
