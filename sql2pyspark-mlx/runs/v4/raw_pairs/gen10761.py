from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_whole", "id")
w_ordered = Window.orderBy("amount", "id")
w3 = Window.partitionBy("channel", "status").orderBy("amount_filled", "id")
w4 = Window.partitionBy("status", "city").orderBy(F.col("discount_whole").desc(), "id")
w_running = Window.partitionBy("city", "channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-10-07").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.ceil("discount").alias("discount_whole"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.opened_date"), F.col("p.city"))
)

cleaned_orders = (
    enriched
    .filter(F.col("city").isNotNull() & (F.col("amount") < 590))
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "discount_whole",
        "opened_date",
        "city",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

enriched_orders = (
    cleaned_orders
    .select(
        "*",
        F.ntile(4).over(w).alias("discount_whole_bucket"),
        F.ntile(4).over(w_ordered).alias("amount_bucket"),
        F.lag("shipping_fee", 2).over(w3).alias("prev_shipping_fee"),
    )
)

result = (
    enriched_orders
    .withColumn("discount_whole_bucket_2", F.ntile(3).over(w4))
    .withColumn("cumulative_discount_whole", F.sum("discount_whole").over(w_running))
)
