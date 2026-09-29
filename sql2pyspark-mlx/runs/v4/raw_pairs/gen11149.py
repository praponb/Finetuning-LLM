from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("channel", "status")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-02-23").cast("date")) & F.col("store_id").isNull())
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < "2023-03-01"), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w_running))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee", 1).over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("amount") >= 1530, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount_rounded", [0.25, 0.5, 0.75]).alias("amount_rounded_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("sqft_share") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("sqft_share_per_mobile"),
    )
    .orderBy("sqft_share_per_mobile")
)
