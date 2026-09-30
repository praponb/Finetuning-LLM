from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

cleaned = (
    df_sessions
    .filter(~F.col("browser").isin(["Chrome", "Edge", "Firefox"]) & (F.col("started_at") >= F.lit("2023-05-19").cast("date")))
    .select(
        "id",
        "customer_id",
        "country",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.country",
        F.col("p.city"),
        F.col("p.name"),
        F.col("p.age"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= "2025-09-13"), "left_anti")
)

joined = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "a.*",
        F.col("c.country").alias("customer_country"),
        F.col("c.credit_limit"),
        F.col("c.age").alias("customer_age"),
    )
)

latest_match = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

result = (
    latest_match
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "country",
        "city",
        "age",
        "customer_country",
        "credit_limit",
        "customer_age",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("customer_age").desc_nulls_last(), "country")
)
