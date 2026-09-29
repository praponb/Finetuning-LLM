from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("days_since_start_date").asc_nulls_first())
w2 = Window.orderBy("monthly_fee")
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy(F.col("days_since_start_date_percent_rank").asc_nulls_first())
w5 = Window.partitionBy("customer_id", "plan")

filtered = (
    df_subscriptions
    .filter(F.year("start_date") == 2023)
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
)

matched_keys = (
    filtered
    .select("plan", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") != "standard")
        .select("plan", "customer_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.signup_date") >= "2024-12-01")
    .select("src.*", F.col("b.email"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "customer_id",
        "plan",
        F.percent_rank().over(w).alias("days_since_start_date_percent_rank"),
        F.rank().over(w2).alias("monthly_fee_rank"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.round((F.col("monthly_fee_rank") / F.sum("monthly_fee_rank").over(w3)), 4).alias("monthly_fee_rank_share"),
        F.rank().over(w4).alias("days_since_start_date_percent_rank_rank"),
        F.count("days_since_start_date_percent_rank").over(w5).alias("customer_id_count_days_since_start_date_percent_rank"),
    )
)

result = (
    scored
    .select(
        "customer_id",
        F.col("days_since_start_date_percent_rank").alias("DAYS_SINCE_START_DATE_PERCENT_RANK_VAL"),
        "monthly_fee_rank_share",
    )
    .unpivot(
        ids=["customer_id"],
        values=["DAYS_SINCE_START_DATE_PERCENT_RANK_VAL", "monthly_fee_rank_share"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
