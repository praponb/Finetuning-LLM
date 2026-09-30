from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("event_seq").desc_nulls_first(), "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("distinct_job_titles").desc())

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-12").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.trim("email").alias("clean_email"),
    )
)

filtered = (
    base
    .filter(F.length("full_name") < 5)
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Intern", "Engineer"]), "Priority").otherwise("Other"))
)

metrics = (
    filtered
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "job_title_group",
        F.lag("salary").over(w_hist).alias("prev_salary"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

daily = (
    ranked
    .groupBy(
        "job_title",
        F.when(F.col("prev_salary") >= 159000, "PREMIUM").otherwise("STANDARD").alias("prev_salary_tier"),
    )
    .agg(
        F.round(F.avg("prev_salary"), 2).alias("avg_prev_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
    )
    .withColumn("distinct_job_titles_dense_rank", F.dense_rank().over(w3))
)

result = (
    daily
    .select(
        "job_title",
        "prev_salary_tier",
        "avg_prev_salary",
        "job_title_list",
        "distinct_job_titles",
        F.when(F.col("job_title").rlike("^(Director|Analyst)$"), "GROUP_B").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("job_title_zone"),
    )
    .orderBy(F.col("job_title").desc())
)
