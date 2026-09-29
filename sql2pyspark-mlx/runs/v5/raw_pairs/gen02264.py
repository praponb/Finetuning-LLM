from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("days_since_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "browser").orderBy(F.col("country_length").desc(), "id")
w3 = Window.partitionBy("customer_id", "country").orderBy(F.col("cumulative_days_since_started_at").desc_nulls_first())
w4 = Window.partitionBy("customer_id", "country").orderBy("country_length_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("last_known_country_length_bucket", "id")
w6 = Window.partitionBy("customer_id").orderBy("days_since_started_at", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("customer_id").orderBy("last_known_country_length_bucket_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy(F.col("last_known_country_length_bucket_avg_3_prior").desc(), "id")
w9 = Window.partitionBy("customer_id").orderBy("cumulative_days_since_started_at_cume_dist", "id")
w10 = Window.partitionBy("customer_id").orderBy("last_known_country_length_bucket_avg_3_prior", "id")

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-28").cast("date")) & (F.col("country").eqNullSafe(F.lit("TH"))))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

enriched = (
    valid
    .filter(F.col("days_since_started_at").between(39, 73))
    .select(
        "id",
        "customer_id",
        "country",
        "days_since_started_at",
        "resolved_country",
        F.sum("days_since_started_at").over(w).alias("cumulative_days_since_started_at"),
        F.ntile(5).over(w2).alias("country_length_bucket"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "days_since_started_at",
        F.cume_dist().over(w3).alias("cumulative_days_since_started_at_cume_dist"),
        F.last("country_length_bucket", ignorenulls=True).over(w4).alias("last_known_country_length_bucket"),
    )
)

enriched_sessions = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("last_known_country_length_bucket_avg_3_prior", F.avg("last_known_country_length_bucket").over(w6))
)

enriched_sessions_2 = (
    enriched_sessions
    .select(
        "*",
        F.sum("seq_no").over(w7).alias("running_seq_no"),
        F.row_number().over(w8).alias("event_seq"),
        F.lag("last_known_country_length_bucket", 1).over(w9).alias("prev_last_known_country_length_bucket"),
        F.lag("last_known_country_length_bucket", 1).over(w10).alias("prev_last_known_country_length_bucket_2"),
    )
)

result = (
    enriched_sessions_2
    .withColumn("cumulative_days_since_started_at_cume_dist_tier", F.when(F.col("cumulative_days_since_started_at_cume_dist") >= 0.86, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["cumulative_days_since_started_at_cume_dist_tier", "seq_no"], ["cumulative_days_since_started_at_cume_dist_tier"], []],
        "cumulative_days_since_started_at_cume_dist_tier",
        "seq_no",
    )
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("cumulative_days_since_started_at_cume_dist", [0.25, 0.5, 0.75]).alias("cumulative_days_since_started_at_cume_dist_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_last_known_country_length_bucket_2", [0.25, 0.5, 0.75]).alias("prev_last_known_country_length_bucket_2_quartiles"),
        F.grouping("cumulative_days_since_started_at_cume_dist_tier").alias("gid"),
    )
)
