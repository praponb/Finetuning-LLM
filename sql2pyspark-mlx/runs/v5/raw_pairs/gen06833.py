from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "city")
w_hist = Window.partitionBy("city", "customer_id").orderBy(F.col("customer_credit_limit").asc_nulls_last())
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("segment")
w5 = Window.partitionBy("city").orderBy("credit_limit")
w6 = Window.orderBy(F.col("customer_id_stddev_samp_credit_limit").desc_nulls_last())

kept_keys = (
    df_subscriptions
    .select("id", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("id", "plan")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("end_date") > F.lit("2023-11-06").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        F.date_sub("start_date", 87).alias("start_date_minus_87d"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.age"), F.col("d.credit_limit"), F.col("d.segment"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.email").like("%@startup.io"))
    .select(
        "a.customer_id",
        "a.start_date_minus_87d",
        "a.credit_limit",
        "a.segment",
        F.col("b.credit_limit").alias("customer_credit_limit"),
        F.col("b.email"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.customer_id",
        "src.start_date_minus_87d",
        "src.credit_limit",
        "src.email",
        F.col("b.city"),
        F.col("b.email").alias("customer_email"),
        F.col("b.segment").alias("customer_segment"),
    )
)

with_dims_subscriptions = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.credit_limit").alias("customer_credit_limit"),
        F.col("p.email").alias("customer_email_2"),
        F.col("p.segment"),
    )
)

with_windows = (
    with_dims_subscriptions
    .select(
        "customer_id",
        "credit_limit",
        "city",
        "customer_email",
        "customer_credit_limit",
        F.stddev_samp("credit_limit").over(w).alias("customer_id_stddev_samp_credit_limit"),
        F.cume_dist().over(w_hist).alias("customer_credit_limit_cume_dist"),
        F.min("customer_credit_limit").over(w3).alias("customer_id_min_customer_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
        F.cume_dist().over(w5).alias("credit_limit_cume_dist"),
    )
)

result = (
    with_windows
    .withColumn("customer_id_stddev_samp_credit_limit_dense_rank", F.dense_rank().over(w6))
    .withColumn("customer_id_avg_customer_id_stddev_samp_credit_limit", F.avg("customer_id_stddev_samp_credit_limit").over(w))
)
