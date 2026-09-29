from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date_period", "id")
w2 = Window.partitionBy("plan").orderBy("end_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("session_no").desc())
w4 = Window.orderBy(F.col("is_new_burst").asc_nulls_first())

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-10-01").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("status")).alias("status_title"),
        F.date_trunc("month", "end_date").alias("end_date_period"),
    )
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("end_date_period").over(w).isNull() | (F.col("end_date_period") > F.lag("end_date_period").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

remaining_keys = (
    sessionized
    .select("plan", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("end_date") <= "2024-08-01")
        .select("plan", "id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.credit_limit"), F.col("p.name"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.*",
        F.col("c.age"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.name").alias("customer_name"),
    )
)

base_subscriptions = (
    with_dims
    .filter(F.col("end_date").between("2024-09-27", "2025-08-01"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "status_title",
        "is_new_burst",
        "session_no",
        "credit_limit",
        "age",
        "customer_credit_limit",
        "customer_name",
        (F.col("credit_limit") - F.col("is_new_burst")).alias("credit_limit_minus_is_new_burst"),
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
    )
)

with_windows = (
    base_subscriptions
    .select(
        "*",
        F.cume_dist().over(w3).alias("session_no_cume_dist"),
        F.percent_rank().over(w3).alias("session_no_percent_rank"),
        F.dense_rank().over(w4).alias("is_new_burst_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status_title",
        "age",
        "customer_credit_limit",
        "session_no_cume_dist",
    )
)
