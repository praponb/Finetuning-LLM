from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("segment", "city").orderBy("credit_limit_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("credit_limit", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("segment").orderBy("running_credit_limit_filled", "id")
w5 = Window.partitionBy("city").orderBy("event_seq", "id")
w6 = Window.orderBy(F.col("running_credit_limit_filled").desc_nulls_last())
w7 = Window.partitionBy("city")
w_hist = Window.partitionBy("city").orderBy("first_known_credit_limit_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("city").orderBy(F.col("city_count_first_known_credit_limit_filled").desc(), "id")

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-06-12").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.concat_ws("/", "city", "country").alias("city_country_key"),
    )
)

cleaned = (
    valid
    .filter(F.col("city").isin(["London", "Tokyo"]))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "credit_limit",
        "city_country_key",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.when(F.col("credit_limit") >= 12600, "High").when(F.col("credit_limit") >= 2500, "Medium").otherwise("Low").alias("credit_limit_level"),
    )
)

metrics = (
    cleaned
    .filter(F.col("credit_limit_filled") <= 28)
    .withColumn("first_known_credit_limit_filled", F.first("credit_limit_filled", ignorenulls=True).over(w_ordered))
    .withColumn("running_credit_limit_filled", F.sum("credit_limit_filled").over(w_recent))
    .withColumn("credit_limit_avg_3_prior", F.avg("credit_limit").over(w3))
)

enriched = (
    metrics
    .select(
        "id",
        "city",
        "credit_limit",
        "city_country_key",
        "first_known_credit_limit_filled",
        "running_credit_limit_filled",
        F.lead("credit_limit_avg_3_prior").over(w4).alias("next_credit_limit_avg_3_prior"),
        F.lag("running_credit_limit_filled").over(w4).alias("prev_running_credit_limit_filled"),
        F.lead("running_credit_limit_filled").over(w4).alias("next_running_credit_limit_filled"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "city",
        "first_known_credit_limit_filled",
        F.lead("first_known_credit_limit_filled", 1).over(w5).alias("next_first_known_credit_limit_filled"),
        F.rank().over(w6).alias("running_credit_limit_filled_rank"),
        F.count("first_known_credit_limit_filled").over(w7).alias("city_count_first_known_credit_limit_filled"),
    )
)

enriched_customers = (
    with_windows
    .select(
        "id",
        "first_known_credit_limit_filled",
        "next_first_known_credit_limit_filled",
        F.avg("first_known_credit_limit_filled").over(w_hist).alias("running_avg_first_known_credit_limit_filled"),
        F.max("first_known_credit_limit_filled").over(w_hist).alias("running_max_first_known_credit_limit_filled"),
        F.count("first_known_credit_limit_filled").over(w_hist).alias("running_count_first_known_credit_limit_filled"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

base = (
    enriched_customers
    .filter(F.col("running_count_first_known_credit_limit_filled") <= 6)
    .select(
        "id",
        "next_first_known_credit_limit_filled",
        "running_avg_first_known_credit_limit_filled",
        "running_max_first_known_credit_limit_filled",
        F.when(F.col("first_known_credit_limit_filled") >= 80, "High").when(F.col("first_known_credit_limit_filled") >= 15, "Medium").otherwise("Low").alias("first_known_credit_limit_filled_band"),
        F.coalesce("running_avg_first_known_credit_limit_filled", F.lit(0)).alias("running_avg_first_known_credit_limit_filled_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("running_max_first_known_credit_limit_filled") >= 70, "PREMIUM").otherwise("STANDARD").alias("running_max_first_known_credit_limit_filled_tier"),
    )
    .agg(
        F.min("running_avg_first_known_credit_limit_filled_filled").alias("min_running_avg_first_known_credit_limit_filled_filled"),
        F.count("*").alias("event_count"),
        F.percentile_approx("running_max_first_known_credit_limit_filled", 0.95).alias("running_max_first_known_credit_limit_filled_p95"),
        F.min("running_avg_first_known_credit_limit_filled").alias("min_running_avg_first_known_credit_limit_filled"),
    )
    .orderBy(
        F.col("running_max_first_known_credit_limit_filled_tier").desc_nulls_first(),
    )
)
