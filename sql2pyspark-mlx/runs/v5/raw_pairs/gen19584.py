from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.max("created_at").alias("ended_at"),
        F.count("*").alias("event_count"),
        F.min("created_at").alias("first_seen"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("activity_day").asc_nulls_last())
)
