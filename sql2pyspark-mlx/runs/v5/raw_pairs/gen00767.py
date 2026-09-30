from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("city", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("signup_date", "id")
w4 = Window.orderBy("credit_limit_filled", "id")
w5 = Window.partitionBy("city").orderBy(F.col("running_sum_credit_limit").desc_nulls_last())

latest = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .filter(F.col("latest_rank") >= 1)
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.count("credit_limit").over(w2).alias("running_count_credit_limit"),
        F.min("credit_limit").over(w2).alias("running_min_credit_limit"),
        F.sum("credit_limit").over(w2).alias("running_sum_credit_limit"),
        F.lead("age", 1).over(w3).alias("next_age"),
    )
)

base = (
    scored
    .filter(F.col("signup_date") >= F.lit("2023-03-23").cast("date"))
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        "running_count_credit_limit",
        "running_min_credit_limit",
        "running_sum_credit_limit",
        F.date_format("signup_date", "MMM yyyy").alias("signup_date_label"),
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.lead("running_min_credit_limit", 1, 0).over(w4).alias("next_running_min_credit_limit"),
        F.rank().over(w5).alias("running_sum_credit_limit_rank"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isin([5, 9, 11, 13])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "signup_date",
        "credit_limit",
        "running_count_credit_limit",
        "running_sum_credit_limit",
        "credit_limit_filled",
        "city_filled",
        "next_running_min_credit_limit",
    )
    .orderBy(F.col("running_sum_credit_limit").desc(), "credit_limit")
)
