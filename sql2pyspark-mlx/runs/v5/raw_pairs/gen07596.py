from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("cumulative_credit_limit").asc_nulls_last())
w4 = Window.partitionBy("country").orderBy(F.col("is_new_york").desc_nulls_last())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-04-04").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

with_windows = (
    cleaned
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "age_plus_credit_limit",
        "is_active_label",
        F.min("age").over(w).alias("country_min_age"),
        F.sum("credit_limit").over(w2).alias("cumulative_credit_limit"),
    )
)

cleaned_customers = (
    with_windows
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "country_min_age",
        "cumulative_credit_limit",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

enriched = (
    cleaned_customers
    .select(
        "id",
        "email",
        "country",
        "country_min_age",
        "cumulative_credit_limit",
        F.cume_dist().over(w_recent).alias("cumulative_credit_limit_cume_dist"),
        F.dense_rank().over(w4).alias("is_new_york_dense_rank"),
    )
)

result = (
    enriched
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("cnt"),
    )
)
