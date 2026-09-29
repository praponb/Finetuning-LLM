from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("status", "plan").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("credit_limit").desc(), "id")
w4 = Window.orderBy("credit_limit", "id")
w5 = Window.partitionBy("customer_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.signup_date"), F.col("c.name"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.credit_limit"), F.col("c.age"))
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

cleaned = (
    latest
    .filter((F.col("start_date") >= F.lit("2024-02-29").cast("date")) & F.col("customer_id").isin([30, 32]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "signup_date",
        "name",
        "credit_limit",
        "age",
        "dedup_rank",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w3).alias("credit_limit_bucket"),
        F.row_number().over(w4).alias("event_seq"),
        F.sum("credit_limit").over(w5).alias("running_credit_limit"),
    )
)

attributed = (
    with_windows
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    attributed
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "name",
        "age",
        "dedup_rank",
    )
    .orderBy(F.col("dedup_rank").asc_nulls_last())
)
