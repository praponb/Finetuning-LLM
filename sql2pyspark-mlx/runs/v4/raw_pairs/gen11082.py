from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first(), "id")
w_part = Window.partitionBy("segment")
w3 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-09"))) & F.col("segment").isNotNull())
    .withColumn("email_prefix", F.substring("email", 1, 3))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

valid = (
    base
    .filter(F.col("signup_date") >= F.lit("2023-03-07").cast("date"))
    .withColumn("segment_length", F.length("segment"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .withColumn("segment_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w_part))
    .withColumn("segment_length_avg_6", F.avg("segment_length").over(w3))
)

summary = (
    scored
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.round(F.avg("segment_length"), 2).alias("avg_segment_length"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
)

result = (
    summary
    .select("week_start", "avg_segment_length", "jp_events")
    .orderBy(F.col("week_start").asc_nulls_first())
)
