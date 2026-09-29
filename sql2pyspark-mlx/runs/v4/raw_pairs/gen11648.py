from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date_period", "id")
w2 = Window.partitionBy("country").orderBy("signup_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").asc_nulls_first())
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("segment", "city").orderBy(F.col("is_active_flag").asc_nulls_first())
w7 = Window.partitionBy("country").orderBy(F.col("prev_signup_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("country").orderBy(F.col("prev_signup_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.date_trunc("WEEK", "signup_date").alias("signup_date_period"),
    )
)

prev_events = cleaned.withColumn("prev_signup_date_period", F.lag("signup_date_period").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_signup_date_period").isNull() | (F.col("signup_date_period") > F.col("prev_signup_date_period") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_customers = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

matched_keys = (
    sessions_customers
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.upper(F.trim("email")) == "DAVID.LEE@GMAIL.COM")
        .select("id")
    )
)

reconciled = (
    sessions_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("segment_stddev_samp_session_start_flag", F.stddev_samp("session_start_flag").over(w3))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w4))
    .withColumn("country_sum_age", F.sum("age").over(w5))
    .withColumn("is_active_flag_rank", F.rank().over(w6))
)

result = (
    enriched
    .withColumn("country_avg_session_start_flag", F.avg("session_start_flag").over(w5))
    .withColumn("credit_limit_avg_7d", F.avg("credit_limit").over(w7))
    .withColumn("is_active_flag_avg_30d", F.avg("is_active_flag").over(w8))
    .withColumn("segment_max_is_active_flag", F.max("is_active_flag").over(w3))
)
