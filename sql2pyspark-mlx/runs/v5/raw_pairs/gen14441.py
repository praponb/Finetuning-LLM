from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Intern", "Manager"]), "Tier 1").otherwise("Other"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("UNKNOWN")))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    latest
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "left")
    .filter(F.length(F.col("p2.email")) < 10)
    .select(
        "e.*",
        F.col("p2.hire_date").alias("employee_hire_date"),
        F.col("p2.full_name").alias("employee_full_name"),
    )
)

base = (
    joined
    .filter((F.col("hire_date") >= F.lit("2023-02-09").cast("date")) & (F.col("row_num") == 1))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "job_title_group",
        "job_title_clean",
        "row_num",
        "employee_hire_date",
        "employee_full_name",
        F.concat("job_title", F.lit("/"), "full_name").alias("job_title_full_name"),
        F.trim("email").alias("clean_email"),
    )
)

result = (
    base
    .withColumn("salary_tier", F.when(F.col("salary") >= 141000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["job_title", "salary_tier"], ["job_title"], []],
        "job_title",
        "salary_tier",
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.stddev_samp("salary").alias("salary_stddev_2"),
        F.round(F.avg("salary"), 2).alias("avg_salary_2"),
        F.grouping("job_title").alias("grouping_level"),
    )
)
