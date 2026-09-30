from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee")
w2 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.orderBy(F.col("end_date").desc(), "id")
w4 = Window.partitionBy("customer_id", "status")
w5 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("monthly_fee_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("first_known_monthly_fee_dense_rank").asc_nulls_first())
w8 = Window.partitionBy("plan")
w9 = Window.partitionBy("status")
w10 = Window.partitionBy("plan").orderBy("running_monthly_fee_dense_rank", "id")
w11 = Window.partitionBy("plan").orderBy("first_known_monthly_fee_dense_rank_rank", "id")

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2024-03-01").cast("date")) & F.col("customer_id").isNotNull() & (F.col("start_date") > "2024-11-01"))
    .select(
        "*",
        F.concat("plan", F.lit(", "), "status").alias("plan_status"),
        F.trim("status").alias("clean_status"),
    )
)

scored = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("monthly_fee_dense_rank") / F.sum("monthly_fee_dense_rank").over(w4)), 4).alias("monthly_fee_dense_rank_share"),
        F.sum("monthly_fee_dense_rank").over(w5).alias("running_monthly_fee_dense_rank"),
        F.first("monthly_fee_dense_rank", ignorenulls=True).over(w6).alias("first_known_monthly_fee_dense_rank"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.signup_date"), F.col("p.age"))
)

scored_subscriptions = (
    with_dims
    .select(
        "*",
        F.rank().over(w7).alias("first_known_monthly_fee_dense_rank_rank"),
        F.sum("monthly_fee_dense_rank").over(w8).alias("plan_sum_monthly_fee_dense_rank"),
        F.max("monthly_fee_dense_rank").over(w9).alias("status_max_monthly_fee_dense_rank"),
        F.row_number().over(w10).alias("row_seq"),
    )
)

metrics = (
    scored_subscriptions
    .select(
        "*",
        F.round((F.col("first_known_monthly_fee_dense_rank_rank") / F.sum("first_known_monthly_fee_dense_rank_rank").over(w8)), 4).alias("first_known_monthly_fee_dense_rank_rank_share"),
        F.row_number().over(w11).alias("seq_no"),
    )
)

filtered = (
    metrics
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-08-14"))) & (F.col("monthly_fee_dense_rank_2") == 5))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "plan_status",
        "clean_status",
        "monthly_fee_dense_rank",
        "monthly_fee_dense_rank_2",
        "event_seq",
        "monthly_fee_dense_rank_share",
        "first_known_monthly_fee_dense_rank",
        "signup_date",
        "age",
        "first_known_monthly_fee_dense_rank_rank",
        "plan_sum_monthly_fee_dense_rank",
        "row_seq",
        "first_known_monthly_fee_dense_rank_rank_share",
        "seq_no",
        F.trim("status").alias("clean_status_2"),
    )
)

result = (
    filtered
    .withColumn("week_start", F.date_trunc("WEEK", "end_date"))
    .groupingSets(
        [["week_start", "customer_id"], ["week_start"], []],
        "week_start",
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.sum("monthly_fee_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_dense_rank_per_standard"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("plan_sum_monthly_fee_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("plan_sum_monthly_fee_dense_rank_per_basic"),
    )
    .orderBy(F.col("week_start").desc())
)
