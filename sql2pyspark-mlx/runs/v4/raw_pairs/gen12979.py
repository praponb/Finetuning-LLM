from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w5 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment").orderBy("last_known_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-11-28").cast("date")) & F.col("city").isNotNull() & ~F.col("name").like("%Gra%"))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") >= F.col("s.signup_date")), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < F.col("s.signup_date")), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.sum("age").over(w).alias("cumulative_age"),
        F.ntile(4).over(w2).alias("age_bucket"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share"),
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.last("credit_limit", ignorenulls=True).over(w).alias("last_known_credit_limit"),
    )
)

with_windows = (
    metrics
    .withColumn("age_bucket_2", F.ntile(4).over(w4))
    .withColumn("cumulative_running_credit_limit", F.sum("running_credit_limit").over(w5))
    .withColumn("last_known_credit_limit_share", F.last("credit_limit_share", ignorenulls=True).over(w6))
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("running_credit_limit") >= 72626, "HIGH").otherwise("LOW").alias("running_credit_limit_tier"),
    )
    .agg(
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("age_bucket_2")).otherwise(0)).alias("smb_age_bucket_2"),
    )
    .filter(F.col("smb_count") < 1)
)

result = (
    daily
    .select(
        "running_credit_limit_tier",
        "consumer_events",
        "smb_count",
        "smb_age_bucket_2",
    )
    .orderBy(F.col("consumer_events").asc_nulls_last())
)
