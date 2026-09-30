from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost_rounded").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("cost_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("cost", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("cost_rounded").desc(), "id")
w5 = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id")
w6 = Window.orderBy("cost_rounded")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-06-30").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.round("cost", 2).alias("cost_rounded"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.last("cost_rounded", ignorenulls=True).over(w2).alias("last_known_cost_rounded"),
        F.lead("cost", 1).over(w_part).alias("next_cost"),
        F.ntile(5).over(w4).alias("cost_rounded_bucket"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.*",
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.status").alias("order_status"),
    )
)

with_windows = (
    enriched
    .withColumn("prev_cost", F.lag("cost", 1, 0).over(w5))
    .withColumn("cost_rounded_dense_rank", F.dense_rank().over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "cost_rounded",
        "carrier_clean",
        "rn",
        "last_known_cost_rounded",
    )
)
