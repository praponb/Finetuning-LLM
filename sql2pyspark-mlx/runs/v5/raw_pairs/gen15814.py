from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy("discount")
w3 = Window.partitionBy("product_id", "status")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

remaining_keys = (
    cleaned
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([19, 22]))
        .select("product_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status"), F.col("d.order_date"), F.col("d.discount"))
)

scored = (
    enriched
    .select(
        "*",
        F.count("discount").over(w).alias("order_id_count_discount"),
        F.cume_dist().over(w2).alias("discount_cume_dist"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

daily = (
    scored
    .groupBy(
        F.date_trunc("week", "order_date").alias("week_start"),
        F.date_trunc("month", "order_date").alias("activity_month"),
    )
    .agg(
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("order_id_count_discount"))).alias("pending_total_order_id_count_discount"),
        F.percentile_approx("discount_cume_dist", 0.9).alias("discount_cume_dist_p90"),
    )
)

result = (
    daily
    .select(
        "week_start",
        "activity_month",
        "pending_total_order_id_count_discount",
        "discount_cume_dist_p90",
    )
    .orderBy("week_start", F.col("activity_month").desc())
)
