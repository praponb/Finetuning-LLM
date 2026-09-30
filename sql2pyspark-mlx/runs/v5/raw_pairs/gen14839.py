from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.col("quantity").cast("string").alias("quantity_num"))
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.unit_price",
        F.col("b.discount"),
        F.col("b.order_date"),
        F.col("b.amount"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.status"))
)

joined = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.amount").alias("order_amount"),
        F.col("b.order_date").alias("order_order_date"),
    )
)

enriched_order_items = (
    joined
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.order_date",
        "a.order_amount",
        "a.order_order_date",
        F.col("o.channel"),
        F.col("o.order_date").alias("order_order_date_2"),
    )
)

cleaned = (
    enriched_order_items
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "order_id",
        "order_date",
        "order_order_date",
        "order_order_date_2",
        F.when(F.col("order_amount") >= 1470, "High").when(F.col("order_amount") >= 680, "Medium").otherwise("Low").alias("order_amount_level"),
    )
)

result = (
    cleaned
    .withColumn("activity_month", F.date_trunc("MONTH", "order_order_date_2"))
    .cube("activity_month", "order_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("cnt") > 1)
)
