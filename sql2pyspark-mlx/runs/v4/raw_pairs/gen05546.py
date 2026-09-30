from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id")
w3 = Window.partitionBy("city").orderBy("credit_limit", "id")
w4 = Window.partitionBy("city").orderBy("prev_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("prev_age", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("age", "id")
w8 = Window.orderBy("prev_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("segment").orderBy("age_bucket", "id")
w10 = Window.partitionBy("segment").orderBy(F.col("prev_age").desc_nulls_last())
w11 = Window.partitionBy("country")
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w13 = Window.partitionBy("segment").orderBy(F.col("next_credit_limit").desc_nulls_last())
w_ordered = Window.partitionBy("segment").orderBy("prev_prev_age", "id")
w_grp = Window.partitionBy("city").orderBy("first_known_age", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("segment")
w_hist_2 = Window.partitionBy("city", "segment")

scored = (
    df_customers
    .select(
        "*",
        F.ntile(5).over(w).alias("age_bucket"),
        F.lag("age", 1, 0).over(w2).alias("prev_age"),
        F.lead("credit_limit", 1).over(w3).alias("next_credit_limit"),
    )
)

base = (
    scored
    .filter(F.col("signup_date") > F.lit("2023-09-27").cast("date"))
    .select("*", F.trim("segment").alias("clean_segment"))
)

with_windows = (
    base
    .select(
        "*",
        F.last("credit_limit", ignorenulls=True).over(w4).alias("last_known_credit_limit"),
        F.avg("age_bucket").over(w5).alias("age_bucket_avg_6"),
        F.first("age", ignorenulls=True).over(w6).alias("first_known_age"),
        F.lag("prev_age", 1).over(w7).alias("prev_prev_age"),
        F.sum("next_credit_limit").over(w8).alias("cumulative_next_credit_limit"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("prev_prev_age", 1).over(w_part).alias("prev_prev_prev_age"),
        F.rank().over(w10).alias("prev_age_rank"),
        F.round((F.col("prev_prev_age") / F.sum("prev_prev_age").over(w11)), 4).alias("prev_prev_age_share"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.balance"))
)

with_windows_customers = (
    with_last
    .withColumn("next_credit_limit_dense_rank", F.dense_rank().over(w13))
    .withColumn("prev_prev_age_bucket", F.ntile(5).over(w_ordered))
    .withColumn("age_bucket_avg_6_avg_3", F.avg("age_bucket_avg_6").over(w_grp))
    .withColumn("segment_count_age_bucket_avg_6", F.count("age_bucket_avg_6").over(w_hist))
    .withColumn("city_min_balance", F.min("balance").over(w_hist_2))
)

base_customers = (
    with_windows_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-15").cast("date")) & (F.col("status") == "FROZEN"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "age_bucket",
        "prev_age",
        "next_credit_limit",
        "clean_segment",
        "last_known_credit_limit",
        "age_bucket_avg_6",
        "prev_prev_age",
        "cumulative_next_credit_limit",
        "prev_prev_prev_age",
        "prev_age_rank",
        "prev_prev_age_share",
        "status",
        "balance",
        "next_credit_limit_dense_rank",
        "prev_prev_age_bucket",
        "age_bucket_avg_6_avg_3",
        "segment_count_age_bucket_avg_6",
        F.round("first_known_age", 1).alias("first_known_age_rounded"),
        F.round((F.col("next_credit_limit_dense_rank") * 0.9), 2).alias("next_credit_limit_dense_rank_adjusted"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    base_customers
    .groupBy("country", "status")
    .pivot("city", ["New York", "Singapore"])
    .agg(F.sum("prev_prev_age_bucket"))
    .select(
        "country",
        "status",
        F.col("New York").alias("new_york"),
        F.col("Singapore").alias("singapore"),
    )
)
