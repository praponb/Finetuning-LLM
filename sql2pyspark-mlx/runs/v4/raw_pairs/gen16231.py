from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("session_country")
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.partitionBy("city").orderBy("age", "id")
w_hist = Window.partitionBy("browser").orderBy("age_share", "id")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").desc(), "id")
w8 = Window.partitionBy("browser")

with_last = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser"), F.col("c.country").alias("session_country"))
)

kept = (
    with_last
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isin([11, 17, 22, 23])), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") <= F.col("s.signup_date")), "left_semi")
)

scored = (
    eligible
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.avg("age").over(w3).alias("age_avg_6"),
    )
)

kept_customers = (
    scored
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device").isin(["mobile", "desktop", "tablet"])), "left_semi")
)

with_windows = (
    kept_customers
    .withColumn("age_bucket", F.ntile(10).over(w_part))
    .withColumn("next_credit_limit", F.lead("credit_limit", 1).over(w_hist))
    .withColumn("age_share_2", F.round((F.col("age") / F.sum("age").over(w6)), 4))
    .withColumn("credit_limit_bucket", F.ntile(10).over(w7))
    .withColumn("browser_max_age_avg_6", F.max("age_avg_6").over(w8))
)

cleaned = (
    with_windows
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-11-22"))) & F.col("session_country").isNotNull() & (F.col("browser") == "Safari"))
    .select(
        "*",
        F.greatest("age_avg_6", "age_bucket").alias("greatest_age_avg_6_age_bucket"),
        F.concat_ws("/", "browser", "name").alias("browser_name_key"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

result = (
    cleaned
    .groupBy("session_country")
    .agg(
        F.round(F.sum("age_bucket") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("age_bucket_per_chrome"),
        F.avg("next_credit_limit").alias("avg_next_credit_limit"),
        F.count_if(F.col("session_country") == "UK").alias("uk_events"),
        F.sum(F.when(F.col("country") == "SG", F.col("age_share_2")).otherwise(0)).alias("sg_age_share_2"),
    )
)
