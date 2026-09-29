from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        F.explode("coupon_codes").alias("value"),
    )
)

enriched = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.signup_date"), F.col("d.city"))
)

enriched_carts = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city").alias("customer_city"))
)

result = (
    enriched_carts
    .rollup(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("customer_city") == "Berlin", 1)).alias("berlin_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
    )
    .orderBy("span_minutes")
)
