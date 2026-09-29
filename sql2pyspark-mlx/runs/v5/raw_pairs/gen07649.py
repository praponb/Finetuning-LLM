from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first(), "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")

valid = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.min("credit_limit").over(w2).alias("city_min_credit_limit"),
        F.ntile(3).over(w3).alias("credit_limit_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("credit_limit") >= 14000, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.avg(F.when(F.col("segment") == "SMB", F.col("credit_limit_bucket"))).alias("smb_avg_credit_limit_bucket"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
    )
)
