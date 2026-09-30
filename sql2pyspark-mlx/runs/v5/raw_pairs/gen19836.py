from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "product_id",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

enriched = (
    lines
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.is_active"))
    .select("s.*", F.col("c.country"))
)

base = (
    enriched
    .filter((~F.col("country").eqNullSafe(F.lit("JP"))) & (F.col("created_at") > F.lit("2025-03-01 13:00:00").cast("timestamp")))
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "element",
        "country",
        F.upper("element").alias("element_upper"),
        F.hour("created_at").alias("created_at_hour"),
    )
)

result = (
    base
    .withColumn("activity_day", F.date_trunc("day", "created_at"))
    .groupingSets(
        [["activity_day", "customer_id"], ["activity_day"]],
        "activity_day",
        "customer_id",
    )
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.min("created_at").alias("first_seen"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("element") == "VIP", 1)).alias("vip_rows"),
        F.min("created_at").alias("first_created_at_2"),
    )
)
