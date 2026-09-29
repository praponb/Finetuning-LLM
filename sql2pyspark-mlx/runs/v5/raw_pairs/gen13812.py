from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (~F.col("job_title").eqNullSafe(F.lit("Analyst"))))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.concat("email", F.lit("/"), "level").alias("email_level"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid_employees = (
    latest
    .filter(F.col("job_title") != "Intern")
    .select("*", F.round("rank_in_group", 1).alias("rank_in_group_rounded"))
)

base = (
    valid_employees
    .filter(F.col("job_title") == "Designer")
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "rank_in_group",
        "rank_in_group_rounded",
        F.round((F.col("salary") * 1.1), 2).alias("salary_scaled"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

filtered = (
    base
    .filter(F.col("salary_scaled") > 74)
    .select(
        "*",
        (F.col("salary") - F.col("rank_in_group")).alias("salary_minus_rank_in_group"),
        F.lower("email").alias("email_lower"),
    )
)

latest_employees = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

totals = (
    latest_employees
    .groupBy(
        F.when(F.col("rank_in_group_rounded") >= 75, "HIGH").otherwise("LOW").alias("rank_in_group_rounded_tier"),
    )
    .agg(
        F.max(F.when(F.col("job_title") == "Intern", F.col("rank_in_group_rounded"))).alias("intern_max_rank_in_group_rounded"),
        F.percentile_approx("salary_scaled", [0.25, 0.5, 0.75]).alias("salary_scaled_quartiles"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("rank_in_group_rounded")).otherwise(0)).alias("engineer_rank_in_group_rounded"),
    )
)

result = (
    totals
    .select(
        "rank_in_group_rounded_tier",
        "intern_max_rank_in_group_rounded",
        "salary_scaled_quartiles",
        "designer_events",
        "job_title_list",
        "engineer_rank_in_group_rounded",
    )
    .orderBy(F.col("designer_events").asc_nulls_last())
)
