from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("segment", "city")
w3 = Window.partitionBy("city", "segment")
w_hist = Window.orderBy(F.col("last4_count_credit_limit").desc(), "id")
w_running = Window.partitionBy("city").orderBy("seq_no", "id")
w6 = Window.partitionBy("country").orderBy("last4_avg_credit_limit", "id").rowsBetween(-3, -1)

scored = (
    df_customers
    .select(
        "*",
        F.min("credit_limit").over(w).alias("last4_min_credit_limit"),
        F.count("credit_limit").over(w).alias("last4_count_credit_limit"),
        F.avg("credit_limit").over(w).alias("last4_avg_credit_limit"),
    )
)

filtered = (
    scored
    .filter(F.col("signup_date") >= F.lit("2023-04-16").cast("date"))
    .select(
        "*",
        F.upper("email").alias("email_upper"),
        F.round("last4_count_credit_limit", 2).alias("last4_count_credit_limit_rounded"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_windows = (
    filtered
    .filter(F.col("segment") == "Consumer")
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.max("last4_min_credit_limit").over(w3).alias("city_max_last4_min_credit_limit"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

with_windows_customers = (
    with_windows
    .select(
        "id",
        "email",
        "country",
        "city",
        "is_active",
        "last4_min_credit_limit",
        "last4_avg_credit_limit",
        "last4_count_credit_limit_rounded",
        "resolved_city",
        "credit_limit_share",
        "city_max_last4_min_credit_limit",
        F.lead("age").over(w_running).alias("next_age"),
        F.lag("age").over(w_running).alias("prev_age"),
        F.lead("last4_min_credit_limit").over(w_running).alias("next_last4_min_credit_limit"),
        F.lag("last4_min_credit_limit").over(w_running).alias("prev_last4_min_credit_limit"),
        F.row_number().over(w_running).alias("seq_no_2"),
        F.avg("city_max_last4_min_credit_limit").over(w6).alias("city_max_last4_min_credit_limit_avg_3_prior"),
    )
)

grouped = (
    with_windows_customers
    .groupBy("country")
    .agg(
        F.round(F.sum("city_max_last4_min_credit_limit") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("city_max_last4_min_credit_limit_per_sg"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("city_max_last4_min_credit_limit").alias("max_city_max_last4_min_credit_limit"),
        F.round(F.sum("next_age") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("next_age_per_new_york"),
    )
)

result = (
    grouped
    .select(
        "country",
        "city_max_last4_min_credit_limit_per_sg",
        "distinct_ids",
        "max_city_max_last4_min_credit_limit",
        "next_age_per_new_york",
        F.when(F.col("country").rlike("^(US|JP)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("country_zone"),
    )
)
