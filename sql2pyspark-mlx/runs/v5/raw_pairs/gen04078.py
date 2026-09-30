from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

matched_keys = (
    df_sessions
    .select("id", "browser")
    .subtract(
        df_sessions
        .filter(~F.col("country").isin(["JP", "SG"]))
        .select("id", "browser")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_semi")
)

kept_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < "2023-07-01"), "left_anti")
)

joined = (
    kept_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.customer_id", "a.browser", "a.country", F.col("d.segment"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.month(F.col("b.signup_date")).isin([2, 4, 8]))
    .select(
        "e.customer_id",
        "e.browser",
        F.col("b.age"),
        F.col("b.name"),
        F.col("b.country").alias("customer_country"),
    )
)

result = (
    with_dims
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "browser",
        "age",
        "name",
        "customer_country",
        F.col("avg_order_discount"),
    )
    .orderBy("browser", "avg_order_discount")
)
