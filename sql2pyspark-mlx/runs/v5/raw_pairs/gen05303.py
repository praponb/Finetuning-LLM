from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan", "status").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("running_max_prev_monthly_fee", "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("city").orderBy("running_max_prev_monthly_fee_bucket", "id")
w6 = Window.partitionBy("city", "customer_id").orderBy("running_max_prev_monthly_fee_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("seq_no_tier").orderBy(F.col("avg_cumulative_running_max_prev_monthly_fee_bucket").desc())

scored = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.lag("monthly_fee").over(w_grp).alias("prev_monthly_fee"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "end_date",
        "prev_monthly_fee",
        "row_seq",
        F.count("prev_monthly_fee").over(w2).alias("running_count_prev_monthly_fee"),
        F.sum("prev_monthly_fee").over(w2).alias("running_sum_prev_monthly_fee"),
        F.min("prev_monthly_fee").over(w2).alias("running_min_prev_monthly_fee"),
        F.max("prev_monthly_fee").over(w2).alias("running_max_prev_monthly_fee"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "customer_id",
        "end_date",
        "running_count_prev_monthly_fee",
        "running_max_prev_monthly_fee",
        F.ntile(5).over(w3).alias("running_max_prev_monthly_fee_bucket"),
        F.round((F.col("running_min_prev_monthly_fee") / F.sum("running_min_prev_monthly_fee").over(w4)), 4).alias("running_min_prev_monthly_fee_share"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.running_max_prev_monthly_fee",
        "s.running_max_prev_monthly_fee_bucket",
        F.col("b.city"),
        F.col("b.age"),
    )
)

scored_subscriptions = (
    joined
    .select(
        "*",
        F.lag("running_max_prev_monthly_fee_bucket").over(w5).alias("prev_running_max_prev_monthly_fee_bucket"),
        F.lead("age").over(w5).alias("next_age"),
        F.lag("age").over(w5).alias("prev_age"),
        F.row_number().over(w5).alias("seq_no"),
        F.sum("running_max_prev_monthly_fee_bucket").over(w6).alias("cumulative_running_max_prev_monthly_fee_bucket"),
    )
)

valid = (
    scored_subscriptions
    .filter(F.col("seq_no") == 4)
    .select(
        "id",
        "customer_id",
        "running_max_prev_monthly_fee",
        "running_max_prev_monthly_fee_bucket",
        "city",
        "age",
        "next_age",
        "seq_no",
        "cumulative_running_max_prev_monthly_fee_bucket",
        F.col("prev_running_max_prev_monthly_fee_bucket").cast("double").alias("prev_running_max_prev_monthly_fee_bucket_num"),
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.age",
        "src.seq_no",
        "src.cumulative_running_max_prev_monthly_fee_bucket",
        F.col("c.signup_date"),
    )
)

totals = (
    with_dims
    .groupBy(
        F.when(F.col("seq_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
        F.when(F.col("cumulative_running_max_prev_monthly_fee_bucket") >= 38, "PREMIUM").otherwise("STANDARD").alias("cumulative_running_max_prev_monthly_fee_bucket_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count("*").alias("cnt_2"),
        F.min("seq_no").alias("min_seq_no"),
        F.avg("cumulative_running_max_prev_monthly_fee_bucket").alias("avg_cumulative_running_max_prev_monthly_fee_bucket"),
    )
    .filter(F.col("cnt") > 1)
    .withColumn("avg_cumulative_running_max_prev_monthly_fee_bucket_dense_rank", F.dense_rank().over(w7))
)

result = (
    totals
    .select(
        "seq_no_tier",
        "cumulative_running_max_prev_monthly_fee_bucket_tier",
        "week_start",
        "avg_seq_no",
        "min_seq_no",
        "avg_cumulative_running_max_prev_monthly_fee_bucket",
        "avg_cumulative_running_max_prev_monthly_fee_bucket_dense_rank",
    )
    .orderBy("min_seq_no", F.col("avg_seq_no").desc())
)
