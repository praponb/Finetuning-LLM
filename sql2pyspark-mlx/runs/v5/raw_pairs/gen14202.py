from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("age_plus_credit_limit", "id")
w_grp = Window.partitionBy("country").orderBy("running_max_credit_limit", "id")
w5 = Window.partitionBy("country").orderBy("running_avg_credit_limit_minus_running_max_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("country").orderBy(F.col("running_avg_credit_limit_minus_running_max_credit_limit").desc_nulls_first())
w8 = Window.partitionBy("country").orderBy("next_running_avg_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-01").cast("date")) & (~F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

scored = (
    base
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "country",
        "credit_limit",
        "age_plus_credit_limit",
        "city_filled",
        F.max("credit_limit").over(w_hist).alias("running_max_credit_limit"),
        F.avg("credit_limit").over(w_hist).alias("running_avg_credit_limit"),
        F.min("credit_limit").over(w_hist).alias("running_min_credit_limit"),
        F.sum("age_plus_credit_limit").over(w_part).alias("cumulative_age_plus_credit_limit"),
    )
)

filtered = (
    scored
    .filter(F.col("country").isin(["TH", "JP", "SG"]))
    .select(
        "*",
        F.round((F.col("running_min_credit_limit") * 1.07), 2).alias("running_min_credit_limit_scaled"),
        (F.col("running_avg_credit_limit") - F.col("running_max_credit_limit")).alias("running_avg_credit_limit_minus_running_max_credit_limit"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("running_max_credit_limit", 1, 0).over(w3).alias("prev_running_max_credit_limit"),
        F.lead("running_avg_credit_limit", 1).over(w_grp).alias("next_running_avg_credit_limit"),
        F.first("running_avg_credit_limit", ignorenulls=True).over(w5).alias("first_known_running_avg_credit_limit"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (~F.col("x.browser").isin(["Edge", "Chrome"])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "credit_limit",
        "city_filled",
        "running_avg_credit_limit",
        "running_min_credit_limit",
        "prev_running_max_credit_limit",
        "next_running_avg_credit_limit",
        "first_known_running_avg_credit_limit",
        F.max("age_plus_credit_limit").over(w6).alias("country_max_age_plus_credit_limit"),
        F.percent_rank().over(w7).alias("running_avg_credit_limit_minus_running_max_credit_limit_percent_rank"),
        F.first("age_plus_credit_limit", ignorenulls=True).over(w8).alias("first_known_age_plus_credit_limit"),
    )
)
