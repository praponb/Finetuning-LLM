from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w_grp = Window.partitionBy("job_title", "level").orderBy("salary", "id")
w3 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("salary", "id")
w7 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-11-23").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level").isin(["L2", "L1"]), "Priority").otherwise("Other").alias("level_group"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_clean"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("salary_avg_3", F.avg("salary").over(w3))
    .withColumn("salary_cume_dist", F.cume_dist().over(w4))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w5))
    .withColumn("salary_bucket", F.ntile(5).over(w6))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    latest
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.dept_name"), F.col("p.budget"), F.col("p.location"))
)

matched_keys = (
    joined
    .select("id", "dept_id")
    .exceptAll(
        df_employees
        .filter(F.col("dept_id").isin([11, 19, 33, 34]))
        .select("id", "dept_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("budget"))).alias("manager_total_budget"),
        F.percentile_approx("salary_bucket", [0.25, 0.5, 0.75]).alias("salary_bucket_quartiles"),
        F.size(F.collect_set("location")).alias("num_locations"),
    )
    .orderBy(F.col("manager_total_budget").desc_nulls_first())
)
