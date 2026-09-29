from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-07-21").cast("date")) & F.col("order_id").isNotNull() & F.col("shipped_date").between("2025-04-01", "2025-05-16"))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
    .withColumn("status_prefix", F.substring("status", 1, 2))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("UNKNOWN")))
)

filtered = (
    cleaned
    .filter((F.col("shipped_date") > F.to_date(F.lit("2024-03-22"))) & F.col("order_id").isNotNull())
    .withColumn("shipped_date_plus_38d", F.date_add("shipped_date", 38))
    .withColumn("status_group", F.when(F.col("status").isin(["LOST", "RETURNED"]), "Tier 1").otherwise("Other"))
    .withColumn("resolved_carrier_2", F.coalesce("carrier", F.lit("UNKNOWN")))
)

enriched = (
    filtered
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.amount"), F.col("d.channel"), F.col("d.discount"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

attributed = (
    deduped
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.shipped_date_plus_38d").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.29, "HIGH").otherwise("LOW"))
    .withColumn("days_shipped_date_to_delivered_date_tier", F.when(F.col("days_shipped_date_to_delivered_date") >= 69, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["discount_pct_tier", "days_shipped_date_to_delivered_date_tier"], ["discount_pct_tier"]],
        "discount_pct_tier",
        "days_shipped_date_to_delivered_date_tier",
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count("*").alias("cnt"),
        F.grouping("discount_pct_tier").alias("grouping_id"),
    )
)
