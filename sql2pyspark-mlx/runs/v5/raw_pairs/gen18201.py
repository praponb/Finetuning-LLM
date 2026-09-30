from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w_recent = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("country_max_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("last7_max_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment")

metrics = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.max("credit_limit").over(w).alias("country_max_credit_limit"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") >= F.col("s.signup_date")), "left_anti")
)

scored = (
    eligible
    .filter(F.col("segment").isin(["Consumer", "Corporate"]))
    .select(
        "*",
        F.sum("row_seq").over(w_recent).alias("last7_sum_row_seq"),
        F.max("row_seq").over(w_recent).alias("last7_max_row_seq"),
        F.min("row_seq").over(w_recent).alias("last7_min_row_seq"),
        F.first("age", ignorenulls=True).over(w4).alias("first_known_age"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.min("country_max_credit_limit").over(w5).alias("running_min_country_max_credit_limit"),
        F.max("country_max_credit_limit").over(w5).alias("running_max_country_max_credit_limit"),
        F.avg("country_max_credit_limit").over(w5).alias("running_avg_country_max_credit_limit"),
        F.corr("credit_limit", "age").over(w6).alias("corr_credit_limit_age"),
    )
)

result = (
    with_windows
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("last7_min_row_seq_tier", F.when(F.col("last7_min_row_seq") >= 6.4, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["segment", "row_seq_tier", "last7_min_row_seq_tier"], ["segment", "row_seq_tier"], ["segment"]],
        "segment",
        "row_seq_tier",
        "last7_min_row_seq_tier",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.percentile_approx("running_max_country_max_credit_limit", [0.25, 0.5, 0.75]).alias("running_max_country_max_credit_limit_quartiles"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.grouping_id().alias("grouping_level"),
    )
)
