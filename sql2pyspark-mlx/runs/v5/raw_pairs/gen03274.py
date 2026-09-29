from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country").orderBy(F.col("city_count_running_avg_credit_limit_str").asc_nulls_last())
w_grp = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_first())
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("country").orderBy(F.col("city_count_running_avg_credit_limit_str").desc_nulls_last())
w_hist = Window.partitionBy("country").orderBy(F.col("running_count_credit_limit").asc_nulls_last())

cleaned = (
    df_customers
    .filter(~F.col("city").eqNullSafe(F.lit("New York")))
    .withColumn("email_lower", F.lower("email"))
)

scored = (
    cleaned
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w_part))
    .withColumn("running_count_credit_limit", F.count("credit_limit").over(w_part))
    .withColumn("running_max_credit_limit", F.max("credit_limit").over(w_part))
    .withColumn("running_avg_credit_limit", F.avg("credit_limit").over(w_part))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w_part_2))
)

kept_keys = (
    scored
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("city").isNull())
        .select("id", "segment")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(~F.col("is_active"))
    .select(
        "name",
        "email",
        "country",
        "credit_limit",
        "email_lower",
        "running_count_credit_limit",
        "first_known_segment",
        F.count("credit_limit").over(w3).alias("city_count_credit_limit"),
        F.count("running_avg_credit_limit").over(w3).alias("city_count_running_avg_credit_limit"),
    )
)

cleaned_customers = (
    enriched
    .filter(F.col("country").isin(["US", "UK"]))
    .select(
        "name",
        "email",
        "country",
        "credit_limit",
        "email_lower",
        "running_count_credit_limit",
        (F.col("city_count_credit_limit") + F.col("city_count_running_avg_credit_limit")).alias("city_count_credit_limit_plus_city_count_running_avg_credit_limit"),
        F.col("city_count_running_avg_credit_limit").cast("double").alias("city_count_running_avg_credit_limit_str"),
    )
)

scored_customers = (
    cleaned_customers
    .select(
        "*",
        F.percent_rank().over(w4).alias("city_count_running_avg_credit_limit_str_percent_rank"),
        F.percent_rank().over(w_grp).alias("credit_limit_percent_rank"),
        F.round((F.col("running_count_credit_limit") / F.sum("running_count_credit_limit").over(w6)), 4).alias("running_count_credit_limit_share"),
        F.dense_rank().over(w7).alias("city_count_running_avg_credit_limit_str_dense_rank"),
        F.dense_rank().over(w_hist).alias("running_count_credit_limit_dense_rank"),
    )
)

valid = (
    scored_customers
    .filter(F.col("country") != "DE")
    .select(
        "name",
        "email",
        "country",
        "credit_limit",
        "email_lower",
        "city_count_credit_limit_plus_city_count_running_avg_credit_limit",
        "credit_limit_percent_rank",
        "city_count_running_avg_credit_limit_str_dense_rank",
        "running_count_credit_limit_dense_rank",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.ceil("running_count_credit_limit").alias("running_count_credit_limit_whole"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("credit_limit_percent_rank") >= 0.74, "HIGH").otherwise("LOW").alias("credit_limit_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count_2"),
        F.min("running_count_credit_limit_whole").alias("min_running_count_credit_limit_whole"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("th_count_2").asc_nulls_last())
)
