from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("age").desc(), "id")
w_ordered = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w_recent = Window.orderBy("age_bucket", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy(F.col("age").asc_nulls_first())
w6 = Window.orderBy(F.col("credit_limit_percent_rank").desc(), "id")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.credit_limit",
        "s.age",
        F.col("c.browser").alias("session_browser"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country").isin(["UK", "TH"])), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "age",
        F.ntile(5).over(w2).alias("age_bucket"),
        F.percent_rank().over(w_ordered).alias("credit_limit_percent_rank"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.device").isin(["mobile", "tablet"])), "left_anti")
)

with_windows = (
    kept
    .filter(F.col("age_bucket") < 5)
    .select(
        "id",
        "age",
        F.row_number().over(w2).alias("seq_no"),
        F.avg("credit_limit_percent_rank").over(w_recent).alias("credit_limit_percent_rank_avg_5"),
        F.ntile(3).over(w2).alias("age_bucket_2"),
        F.cume_dist().over(w_running).alias("age_cume_dist"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("seq_no") >= 6)
    .select(
        "*",
        F.round((F.col("age_cume_dist") * 1.1), 2).alias("age_cume_dist_scaled"),
    )
)

result = (
    valid
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("age_cume_dist_scaled", 0.95).alias("age_cume_dist_scaled_p95"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.round(F.avg("credit_limit_percent_rank_avg_5"), 2).alias("avg_credit_limit_percent_rank_avg_5"),
    )
    .orderBy(F.col("seq_no_p95").desc_nulls_last())
)
