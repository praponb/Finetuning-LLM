from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w_recent = Window.partitionBy("segment", "city").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("signup_date").desc(), "id")
w4 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-08-25"))) & (F.col("credit_limit") >= 13300))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
        F.initcap(F.trim("segment")).alias("segment_title"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .filter(F.col("email").like("%@company.co"))
    .select(
        "*",
        F.min("credit_limit").over(w_recent).alias("running_min_credit_limit"),
        F.count("credit_limit").over(w_recent).alias("running_count_credit_limit"),
        F.sum("credit_limit").over(w_recent).alias("running_sum_credit_limit"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "segment_clean",
        "rank_in_group",
        "running_count_credit_limit",
        "running_sum_credit_limit",
        F.row_number().over(w_grp).alias("seq_no"),
        F.sum("running_min_credit_limit").over(w4).alias("cumulative_running_min_credit_limit"),
    )
)

result = (
    scored
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        "segment_clean",
        "rank_in_group",
        "seq_no",
    )
)
