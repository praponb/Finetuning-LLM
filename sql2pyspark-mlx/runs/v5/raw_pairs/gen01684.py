from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") <= "2025-10-01"))
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.date_format("start_date", "yyyy-MM").alias("start_date_label"),
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

remaining_keys = (
    valid
    .select("status", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isin([10, 18, 20, 36]))
        .select("status", "plan")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        "start_date_label",
        F.trim("status").alias("clean_status"),
        F.date_format("end_date_month_end", "MMM yyyy").alias("end_date_month_end_label"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date_month_end_label",
        F.col("p.email"),
        F.col("p.credit_limit"),
    )
)

result = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "segment"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("p.segment").alias("segment"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.credit_limit")), 2).alias("avg_credit_limit"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
