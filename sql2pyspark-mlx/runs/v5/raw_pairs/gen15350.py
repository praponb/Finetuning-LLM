from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("monthly_fee", "id")

joined = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.end_date",
        "a.monthly_fee",
        "a.status",
        F.col("c.city"),
    )
)

scored = (
    joined
    .select(
        "id",
        "customer_id",
        "city",
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.*",
        F.col("c.age"),
        F.col("c.city").alias("customer_city"),
        F.col("c.credit_limit"),
    )
)

valid = (
    enriched
    .filter((F.col("city").eqNullSafe(F.lit("Chiang Mai"))) & F.col("customer_id").isin([8, 13]))
    .select(
        "id",
        "customer_id",
        "city",
        "running_count_monthly_fee",
        "running_min_monthly_fee",
        "customer_city",
        "credit_limit",
        F.round((F.col("running_count_monthly_fee") / F.col("event_seq")), 2).alias("running_count_monthly_fee_per_event_seq"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("city").isin(["Bangkok", "New York"]))
    .withColumn("running_count_monthly_fee_per_event_seq_level", F.when(F.col("running_count_monthly_fee_per_event_seq") >= 88, "High").when(F.col("running_count_monthly_fee_per_event_seq") >= 11, "Medium").otherwise("Low"))
)

joined_subscriptions = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.country"))
)

valid_subscriptions = (
    joined_subscriptions
    .filter(F.col("city").isin(["Chiang Mai", "Berlin", "New York"]))
    .select(
        "id",
        "city",
        "running_count_monthly_fee",
        "customer_city",
        "credit_limit",
        "city_clean",
        "running_count_monthly_fee_per_event_seq_level",
        (F.col("running_count_monthly_fee_per_event_seq") * F.col("credit_limit")).alias("running_count_monthly_fee_per_event_seq_times_credit_limit"),
        (F.col("running_count_monthly_fee") - F.col("running_count_monthly_fee_per_event_seq")).alias("running_count_monthly_fee_minus_running_count_monthly_fee_per_event_seq"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    valid_subscriptions
    .select(
        "id",
        "city",
        "running_count_monthly_fee",
        "running_count_monthly_fee_per_event_seq_level",
        "running_count_monthly_fee_per_event_seq_times_credit_limit",
        "city_filled",
        F.when(F.col("city").rlike("^(Tokyo|London)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
)
