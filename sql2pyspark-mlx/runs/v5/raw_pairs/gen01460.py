from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w2 = Window.orderBy("order_date", "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-28").cast("date")) & F.col("order_id").isNotNull())
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.paid_at",
        "a.status",
        F.col("b.shipping_fee"),
    )
)

filtered = (
    with_dims
    .filter(~F.col("method").eqNullSafe(F.lit("cash")))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.upper("status").alias("status_upper"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    deduped
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.id", "src.order_id", "src.status", F.col("d.order_date"))
)

metrics = enriched.select("*", F.row_number().over(w2).alias("row_seq"))

result = metrics.select("id", "order_date", "row_seq")
