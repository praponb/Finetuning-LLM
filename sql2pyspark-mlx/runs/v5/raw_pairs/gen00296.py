from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_last())
w3 = Window.partitionBy("segment").orderBy("greatest_credit_limit_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-20").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"),
        F.upper("city").alias("city_upper"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "segment",
        "greatest_credit_limit_age",
        "city_upper",
        F.first("age", ignorenulls=True).over(w).alias("first_known_age"),
        F.dense_rank().over(w2).alias("age_dense_rank"),
        F.first("age", ignorenulls=True).over(w3).alias("first_known_age_2"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (~F.col("c.plan").isin(["standard", "enterprise", "basic"])), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type").alias("account_account_type"))
)

result = (
    with_last
    .withColumn("greatest_credit_limit_age_tier", F.when(F.col("greatest_credit_limit_age") >= 65, "PREMIUM").otherwise("STANDARD"))
    .withColumn("age_dense_rank_tier", F.when(F.col("age_dense_rank") >= 9, "LARGE").otherwise("SMALL"))
    .withColumn("first_known_age_2_tier", F.when(F.col("first_known_age_2") >= 72, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["greatest_credit_limit_age_tier", "age_dense_rank_tier", "first_known_age_2_tier"], ["greatest_credit_limit_age_tier", "age_dense_rank_tier"], ["greatest_credit_limit_age_tier"], []],
        "greatest_credit_limit_age_tier",
        "age_dense_rank_tier",
        "first_known_age_2_tier",
    )
    .agg(
        F.avg(F.when(F.col("account_account_type") == "CHECKING", F.col("age_dense_rank"))).alias("checking_avg_age_dense_rank"),
        F.avg("first_known_age_2").alias("avg_first_known_age_2"),
        F.count(F.when(F.col("account_account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.count_if(F.col("account_account_type") == "LOAN").alias("loan_events"),
        F.avg("first_known_age").alias("avg_first_known_age"),
        F.sum(F.when(F.col("account_account_type") == "CHECKING", F.col("first_known_age")).otherwise(0)).alias("checking_first_known_age"),
    )
    .filter(F.col("savings_count") < 1000)
    .orderBy(F.col("checking_avg_age_dense_rank").desc())
)
