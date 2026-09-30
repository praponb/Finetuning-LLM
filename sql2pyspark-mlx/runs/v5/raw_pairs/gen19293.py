from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-06-02")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.upper("plan").alias("plan_upper"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.email").isNull())
    .select("e.*", F.col("c.name"), F.col("c.city"), F.col("c.age"))
)

enriched_subscriptions = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.signup_date"), F.col("p.segment"))
)

remaining_keys = (
    enriched_subscriptions
    .select("status", "id")
    .intersect(
        df_subscriptions
        .filter(F.year("end_date") >= 2023)
        .select("status", "id")
    )
)

verified = (
    enriched_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.customer_id",
        "s.plan",
        "s.start_date",
        "s.plan_upper",
        "s.name",
        "s.city",
        "s.age",
        "s.signup_date",
        "s.segment",
        F.col("p.age").alias("customer_age"),
        F.col("p.segment").alias("customer_segment"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.customer_id",
        "s.name",
        "s.signup_date",
        "s.segment",
        "s.customer_age",
        F.col("d.segment").alias("customer_segment_2"),
        F.col("d.city").alias("customer_city"),
    )
)

with_dims_subscriptions = (
    with_dims
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.customer_id",
        "e.name",
        "e.signup_date",
        "e.segment",
        "e.customer_segment_2",
        F.col("b.email"),
        F.col("b.city"),
    )
)

daily = (
    with_dims_subscriptions
    .groupBy("segment")
    .agg(
        F.count(F.when(F.col("customer_segment_2") == "SMB", 1)).alias("smb_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("smb_count") > 1)
)

result = daily.select("segment", "city_list")
