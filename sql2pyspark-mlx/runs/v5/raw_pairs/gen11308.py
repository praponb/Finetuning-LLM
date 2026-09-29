from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy("adjusted_credit_limit", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("segment", "city").orderBy("is_active_flag", "id")
w3 = Window.partitionBy("city").orderBy(F.col("prev_adjusted_credit_limit").desc_nulls_first(), "id")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city").orderBy("prev_adjusted_credit_limit", "id")
w6 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("prev_adjusted_credit_limit", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-16").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.round((F.col("credit_limit") * 1.15), 2).alias("adjusted_credit_limit"),
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

enriched = (
    valid
    .filter(F.col("name").like("Ca%"))
    .select(
        "id",
        "email",
        "city",
        "credit_limit",
        "adjusted_credit_limit",
        F.avg("is_active_flag").over(w_grp).alias("is_active_flag_avg_3"),
        F.lag("adjusted_credit_limit", 1).over(w2).alias("prev_adjusted_credit_limit"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "email", "city", "credit_limit", "prev_adjusted_credit_limit")
)

with_windows = (
    latest
    .select(
        "id",
        "prev_adjusted_credit_limit",
        F.stddev_samp("prev_adjusted_credit_limit").over(w4).alias("city_stddev_samp_prev_adjusted_credit_limit"),
        F.lead("prev_adjusted_credit_limit", 1, 0).over(w5).alias("next_prev_adjusted_credit_limit"),
        F.last("city", ignorenulls=True).over(w6).alias("last_known_city"),
        F.avg("credit_limit").over(w7).alias("credit_limit_avg_2_prior"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") == "PENDING"), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_adjusted_credit_limit",
        "s.city_stddev_samp_prev_adjusted_credit_limit",
        "s.last_known_city",
        F.col("c.balance"),
    )
)

stats = (
    latest_match
    .groupBy(
        F.when(F.col("balance") >= 80000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        F.when(F.col("prev_adjusted_credit_limit") >= 76, "LARGE").otherwise("SMALL").alias("prev_adjusted_credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("prev_adjusted_credit_limit", [0.25, 0.5, 0.75]).alias("prev_adjusted_credit_limit_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("city_stddev_samp_prev_adjusted_credit_limit", 0.95).alias("city_stddev_samp_prev_adjusted_credit_limit_p95"),
        F.percentile_approx("prev_adjusted_credit_limit", [0.25, 0.5, 0.75]).alias("prev_adjusted_credit_limit_quartiles_2"),
        F.count("*").alias("num_rows_2"),
    )
)

result = (
    stats
    .select(
        "balance_tier",
        "prev_adjusted_credit_limit_tier",
        "prev_adjusted_credit_limit_quartiles",
        "city_stddev_samp_prev_adjusted_credit_limit_p95",
        "prev_adjusted_credit_limit_quartiles_2",
        "num_rows_2",
    )
)
