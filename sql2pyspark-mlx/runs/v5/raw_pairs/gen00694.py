from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-08-27").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

matched_keys = (
    filtered
    .select("order_id")
    .intersect(
        df_shipments
        .filter(F.col("cost") < 36)
        .select("order_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .filter(~F.col("o.channel").isin(["partner", "web"]))
    .select(
        "a.id",
        "a.order_id",
        "a.status",
        "a.status_group",
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.shipping_fee").alias("order_shipping_fee"))
)

cleaned = (
    joined
    .filter(F.col("shipping_fee") > 31)
    .withColumn("discount_filled", F.coalesce("discount", F.lit(0)))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

result = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["channel", "order_status", "shipment_status"], ["channel", "order_status"], ["channel"], []],
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.amount_int")).alias("avg_amount_int"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("avg_amount_int").desc_nulls_first())
)
