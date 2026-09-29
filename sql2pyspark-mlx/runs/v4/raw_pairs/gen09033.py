from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location")
w5 = Window.orderBy(F.col("budget_bucket").desc_nulls_first())

latest = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.percent_rank().over(w2).alias("budget_percent_rank"),
        F.ntile(4).over(w3).alias("budget_bucket"),
    )
)

matched_keys = (
    scored
    .select("dept_name")
    .exceptAll(
        df_departments
        .filter(F.col("budget") < 1177000)
        .select("dept_name")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("budget_bucket") >= 4)
    .select(
        "id",
        "dept_name",
        "location",
        "budget_percent_rank",
        "budget_bucket",
        F.when(F.col("budget_percent_rank") >= 0.63, "High").when(F.col("budget_percent_rank") >= 0.16, "Medium").otherwise("Low").alias("budget_percent_rank_tier"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id") == 26), "left_semi")
)

scored_departments = (
    not_flagged
    .select(
        "*",
        F.avg("budget_bucket").over(w4).alias("location_avg_budget_bucket"),
        F.sum("budget_bucket").over(w4).alias("location_sum_budget_bucket"),
        F.dense_rank().over(w5).alias("budget_bucket_dense_rank"),
    )
)

cleaned = (
    scored_departments
    .filter(F.col("location_avg_budget_bucket").between(3, 5))
    .select(
        "id",
        "location",
        "location_avg_budget_bucket",
        "location_sum_budget_bucket",
        "budget_bucket_dense_rank",
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
        F.lower("location").alias("location_lower"),
    )
)

daily = (
    cleaned
    .withColumn("location_avg_budget_bucket_tier", F.when(F.col("location_avg_budget_bucket") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("budget_bucket_dense_rank_tier", F.when(F.col("budget_bucket_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["location_avg_budget_bucket_tier", "budget_bucket_dense_rank_tier"], ["location_avg_budget_bucket_tier"]],
        "location_avg_budget_bucket_tier",
        "budget_bucket_dense_rank_tier",
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.round(F.sum("location_sum_budget_bucket") / F.nullif(F.count(F.when(F.col("location") == "Singapore", 1)), F.lit(0)), 2).alias("location_sum_budget_bucket_per_singapore"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
    )
)

result = (
    daily
    .select(
        "budget_bucket_dense_rank_tier",
        "location_sum_budget_bucket_per_singapore",
        "bangkok_rows",
    )
)
