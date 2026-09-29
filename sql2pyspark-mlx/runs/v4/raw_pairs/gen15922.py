from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("credit_limit", "id")
w4 = Window.partitionBy("city").orderBy(F.col("credit_limit_percent_rank").desc())
w5 = Window.orderBy(F.col("first_known_age").desc())
w6 = Window.partitionBy("city").orderBy("credit_limit", "id")
w7 = Window.orderBy(F.col("credit_limit_bucket").desc(), "id")
w8 = Window.partitionBy("country").orderBy(F.col("credit_limit_bucket").desc())
w9 = Window.orderBy("credit_limit_bucket", "id").rowsBetween(-6, -1)

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-12-08").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.trim("city").alias("clean_city"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.browser") == "Safari"), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.percent_rank().over(w).alias("credit_limit_percent_rank"),
        F.first("age", ignorenulls=True).over(w2).alias("first_known_age"),
        F.ntile(4).over(w3).alias("credit_limit_bucket"),
    )
)

valid = (
    scored
    .filter(F.col("first_known_age") <= 36)
    .withColumn("age_str", F.col("age").cast("double"))
    .withColumn("greatest_credit_limit_first_known_age", F.greatest("credit_limit", "first_known_age"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("UNKNOWN")))
)

latest = (
    valid
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .withColumn("first_known_age_cume_dist", F.cume_dist().over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("credit_limit_bucket_bucket", F.ntile(10).over(w7))
    .withColumn("credit_limit_bucket_percent_rank", F.percent_rank().over(w8))
    .withColumn("age_avg_6_prior", F.avg("age").over(w9))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "country",
        "age",
        "clean_city",
        "segment_filled",
        "first_known_age",
        "credit_limit_bucket",
        "greatest_credit_limit_first_known_age",
        F.when(F.col("country").rlike("^(DE|UK)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("age").desc())
)
