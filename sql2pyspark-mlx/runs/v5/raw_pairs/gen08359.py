from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w_grp = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last())

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("cost") > 62))
    .withColumn("carrier_lower", F.lower("carrier"))
    .withColumn("cost_band", F.when(F.col("cost") >= 74, "High").when(F.col("cost") >= 42, "Medium").otherwise("Low"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("DIRECT")))
)

scored = (
    cleaned
    .filter(F.col("shipped_date") > "2023-03-19")
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_bucket", F.ntile(4).over(w2))
    .withColumn("cost_percent_rank", F.percent_rank().over(w_grp))
)

joined = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.delivered_date",
        "src.cost",
        "src.carrier_lower",
        "src.cost_band",
        "src.carrier_clean",
        "src.cost_share",
        "src.cost_bucket",
        F.col("o.status").alias("order_status"),
    )
)

base = (
    joined
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_date_label"),
        F.coalesce("order_status", F.lit("UNKNOWN")).alias("resolved_order_status"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.delivered_date",
        "a.cost",
        "a.order_status",
        "a.cost_scaled",
        "a.delivered_date_label",
        F.col("b.channel"),
    )
)

valid = (
    enriched
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-07-13"))) & F.col("order_id").isNotNull())
    .withColumn("clean_order_status", F.trim("order_status"))
    .withColumn("cost_scaled_rounded", F.round("cost_scaled", 0))
)

result = (
    valid
    .select(
        "id",
        "delivered_date",
        "cost",
        "delivered_date_label",
        "channel",
        "clean_order_status",
        "cost_scaled_rounded",
        F.when(F.col("channel").rlike("^(partner|store)$"), "GROUP_A").otherwise("CORE").alias("channel_zone"),
    )
)
