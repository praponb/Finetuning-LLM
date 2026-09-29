from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city", "segment").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city", "segment")
w3 = Window.partitionBy("segment").orderBy(F.col("greatest_credit_limit_age").asc_nulls_last())
w4 = Window.orderBy(F.col("city_avg_credit_limit").desc())

base = (
    df_customers
    .filter((F.col("city") == "Bangkok") & (F.col("signup_date") > F.lit("2023-03-29").cast("date")))
    .withColumn("greatest_credit_limit_age", F.greatest("credit_limit", "age"))
)

enriched = (
    base
    .withColumn("last7_avg_age", F.avg("age").over(w_recent))
    .withColumn("last7_sum_age", F.sum("age").over(w_recent))
    .withColumn("last7_count_age", F.count("age").over(w_recent))
    .withColumn("city_avg_credit_limit", F.avg("credit_limit").over(w2))
)

kept_keys = (
    enriched
    .select("city")
    .intersect(
        df_customers
        .filter(F.col("signup_date") >= "2024-05-01")
        .select("city")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched_customers = (
    reconciled
    .filter(F.col("last7_sum_age") > 280)
    .select(
        "name",
        "email",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "greatest_credit_limit_age",
        "last7_sum_age",
        "last7_count_age",
        "city_avg_credit_limit",
        F.rank().over(w3).alias("greatest_credit_limit_age_rank"),
        F.dense_rank().over(w4).alias("city_avg_credit_limit_dense_rank"),
    )
)

stats = (
    enriched_customers
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("greatest_credit_limit_age", [0.25, 0.5, 0.75]).alias("greatest_credit_limit_age_quartiles"),
    )
)

result = (
    stats
    .select("week_start", "city_list")
    .orderBy(F.col("week_start").desc())
    .limit(20)
)
