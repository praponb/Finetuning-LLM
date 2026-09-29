from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("city", "country")
w3 = Window.orderBy("age", "id")
w_hist = Window.partitionBy("segment", "country")
w_running = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy(F.col("credit_limit_bucket").asc_nulls_last(), "id")
w7 = Window.partitionBy("country").orderBy(F.col("city_sum_credit_limit").desc_nulls_last(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < F.col("s.signup_date")), "left_semi")
)

with_windows = (
    kept
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "age",
        F.ntile(4).over(w).alias("credit_limit_bucket"),
        F.sum("credit_limit").over(w2).alias("city_sum_credit_limit"),
        F.ntile(5).over(w3).alias("age_bucket"),
        F.corr("credit_limit", "age").over(w_hist).alias("corr_credit_limit_age"),
    )
)

with_windows_customers = (
    with_windows
    .filter(~F.col("is_active"))
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "is_active",
        "credit_limit_bucket",
        "city_sum_credit_limit",
        F.min("credit_limit_bucket").over(w_running).alias("country_min_credit_limit_bucket"),
        F.max("age_bucket").over(w_running).alias("country_max_age_bucket"),
    )
)

deduped = (
    with_windows_customers
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "name", "country", "city_sum_credit_limit")
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

summary = (
    latest
    .groupBy(
        F.when(F.col("city_sum_credit_limit") >= 85056, "HIGH").otherwise("LOW").alias("city_sum_credit_limit_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries"),
    )
)

result = (
    summary
    .select("city_sum_credit_limit_tier", "country_list")
    .orderBy(F.col("city_sum_credit_limit_tier").desc_nulls_first())
)
