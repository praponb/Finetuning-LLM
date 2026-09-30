from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("status_lower", F.lower("status"))
    .withColumn("plan_status", F.concat("plan", F.lit(" - "), "status"))
)

remaining_keys = (
    valid
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date").between("2023-10-01", "2024-04-10"))
        .select("customer_id", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country") == "UK")
    .select("a.*", F.col("c.credit_limit"), F.col("c.email"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.status",
        "e.status_lower",
        "e.email",
        F.col("d.city"),
        F.col("d.name"),
    )
)

result = (
    enriched
    .groupBy("city")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
