from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title", "manager_id")
w6 = Window.partitionBy("job_title")
w7 = Window.partitionBy("job_title").orderBy("salary", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-08"))) & F.col("manager_id").isNotNull())
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("unassigned")))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .filter(F.col("job_title") != "Intern")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        F.avg("salary").over(w2).alias("salary_avg_2"),
        F.last("job_title", ignorenulls=True).over(w3).alias("last_known_job_title"),
        F.sum("salary").over(w4).alias("cumulative_salary"),
    )
)

base = (
    metrics
    .filter(~F.col("job_title").eqNullSafe(F.lit("Manager")))
    .select(
        "*",
        F.round((F.col("salary") * 0.9), 2).alias("salary_adjusted"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.sum("cumulative_salary").over(w5).alias("job_title_sum_cumulative_salary"),
        F.sum("cumulative_salary").over(w6).alias("job_title_sum_cumulative_salary_2"),
        F.lead("salary_adjusted", 2).over(w7).alias("next_salary_adjusted"),
    )
)

kept_keys = (
    enriched
    .select("id", "job_title")
    .exceptAll(
        df_employees
        .filter(~F.col("job_title").isin(["Manager", "Intern"]))
        .select("id", "job_title")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

summary = (
    verified
    .rollup("manager_id")
    .agg(
        F.percentile_approx("salary_adjusted", 0.5).alias("salary_adjusted_p50"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.percentile_approx("job_title_sum_cumulative_salary_2", [0.25, 0.5, 0.75]).alias("job_title_sum_cumulative_salary_2_quartiles"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.grouping("manager_id").alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "num_job_titles",
        "job_title_sum_cumulative_salary_2_quartiles",
        "manager_rows",
        "grouping_level",
    )
)
