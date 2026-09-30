from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc())
w_hist = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.email").isNotNull())
    .select("e.*", F.col("p.segment"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.end_date",
        "a.monthly_fee",
        "a.status",
        "a.segment",
        F.col("p.signup_date"),
        F.col("p.country"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.email"), F.col("p.age"), F.col("p.credit_limit"))
)

deduped = (
    enriched
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "email",
        "age",
        "credit_limit",
    )
)

with_windows = (
    deduped
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "email",
        F.sum("credit_limit").over(w_hist).alias("running_sum_credit_limit"),
        F.max("credit_limit").over(w_hist).alias("running_max_credit_limit"),
        F.count("credit_limit").over(w_hist).alias("running_count_credit_limit"),
        F.sum("credit_limit").over(w3).alias("running_credit_limit"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("end_date") >= F.lit("2023-04-07").cast("date"))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "running_sum_credit_limit",
        "running_max_credit_limit",
        "running_count_credit_limit",
        (F.col("running_credit_limit") * F.col("running_count_credit_limit")).alias("running_credit_limit_times_running_count_credit_limit"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "running_sum_credit_limit",
        "running_max_credit_limit",
        "running_credit_limit_times_running_count_credit_limit",
    )
    .orderBy(
        F.col("running_credit_limit_times_running_count_credit_limit").desc_nulls_first(),
    )
)
