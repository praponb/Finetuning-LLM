from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

with_windows = (
    df_customers
    .select(
        "*",
        F.lead("age").over(w_running).alias("next_age"),
        F.lag("credit_limit").over(w_running).alias("prev_credit_limit"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.device") == "mobile"), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "prev_credit_limit",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

totals = (
    base
    .groupBy("city")
    .agg(
        F.max(F.when(F.col("segment") == "Consumer", F.col("is_active_flag"))).alias("consumer_max_is_active_flag"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count("*").alias("cnt"),
        F.stddev_samp("age").alias("age_stddev"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("credit_limit_per_smb"),
    )
    .filter(F.col("age_stddev") >= 0)
)

result = (
    totals
    .select(
        "city",
        "consumer_max_is_active_flag",
        "chiang_mai_events",
        "age_stddev",
        "credit_limit_per_smb",
        F.when(F.col("city").rlike("^(New York|Singapore)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("consumer_max_is_active_flag").asc_nulls_last())
)
