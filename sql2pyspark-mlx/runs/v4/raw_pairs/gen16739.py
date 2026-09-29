from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("age", "id")
w2 = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("order_status").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("order_status").orderBy("credit_limit_rounded", "id")
w6 = Window.partitionBy("order_status").orderBy("age", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("country").orderBy(F.col("age_bucket_2_rounded").desc(), "id")
w8 = Window.orderBy(F.col("credit_limit_bucket").desc_nulls_last())
w_grp = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w10 = Window.partitionBy("country")

scored = (
    df_customers
    .select(
        "*",
        F.ntile(10).over(w).alias("age_bucket"),
        F.ntile(5).over(w2).alias("age_bucket_2"),
    )
)

valid = (
    scored
    .filter(F.col("signup_date") >= F.lit("2023-12-19").cast("date"))
    .withColumn("age_bucket_2_rounded", F.round("age_bucket_2", 1))
    .withColumn("credit_limit_rounded", F.round("credit_limit", 0))
)

with_latest = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

filtered = (
    with_latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "age_bucket_2",
        "age_bucket_2_rounded",
        "credit_limit_rounded",
        "order_status",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("order_status", F.lit("N/A")).alias("resolved_order_status"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w4).alias("credit_limit_bucket"),
        F.ntile(4).over(w5).alias("credit_limit_rounded_bucket"),
    )
)

with_windows_customers = (
    with_windows
    .withColumn("age_bucket_2_rounded_avg_5_prior", F.avg("age_bucket_2_rounded").over(w6))
    .withColumn("age_bucket_2_rounded_bucket", F.ntile(5).over(w7))
    .withColumn("credit_limit_bucket_percent_rank", F.percent_rank().over(w8))
)

not_flagged = (
    with_windows_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") < F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w_grp).alias("event_seq"),
        F.max("credit_limit_bucket_percent_rank").over(w10).alias("country_max_credit_limit_bucket_percent_rank"),
    )
)
