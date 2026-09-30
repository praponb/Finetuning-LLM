from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("segment").orderBy("start_date", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("monthly_fee_avg_5_prior_whole", "id")
w_part = Window.partitionBy("status").orderBy("monthly_fee_avg_5_prior_whole", "id")
w5 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee_avg_5_prior_whole", "id")
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_avg_5_prior_whole").desc_nulls_last())
w7 = Window.partitionBy("customer_id").orderBy("monthly_fee_avg_5_prior_whole", "id")

with_dims = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.start_date",
        "src.monthly_fee",
        "src.status",
        F.col("p.age"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.start_date",
        "a.monthly_fee",
        "a.status",
        F.col("p.signup_date"),
        F.col("p.email"),
        F.col("p.segment"),
    )
)

enriched_subscriptions = (
    enriched
    .select(
        "id",
        "customer_id",
        "status",
        "email",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
    )
)

filtered = (
    enriched_subscriptions
    .filter(F.col("monthly_fee_avg_5_prior") < 200)
    .select(
        "id",
        "customer_id",
        "status",
        "email",
        "monthly_fee_rank",
        "monthly_fee_avg_5_prior",
        F.concat_ws("-", "status", "email").alias("status_email_key"),
        F.ceil("monthly_fee_avg_5_prior").alias("monthly_fee_avg_5_prior_whole"),
    )
)

cleaned = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        "email",
        "monthly_fee_avg_5_prior_whole",
        F.upper("status").alias("status_upper"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w3).alias("monthly_fee_avg_5_prior_whole_bucket"),
        F.ntile(4).over(w_part).alias("monthly_fee_avg_5_prior_whole_bucket_2"),
        F.row_number().over(w5).alias("row_seq"),
        F.dense_rank().over(w_ordered).alias("monthly_fee_avg_5_prior_whole_dense_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("monthly_fee_avg_5_prior_whole") >= 89, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_avg_5_prior_whole_tier"),
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("row_count"),
    )
)
