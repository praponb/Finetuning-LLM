from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("dept_id")
w5 = Window.orderBy("hire_date", "id")
w6 = Window.partitionBy("level", "dept_id").orderBy("salary_avg_3")
w7 = Window.partitionBy("dept_id").orderBy("salary_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2024-02-28"))) & (F.col("hire_date") >= "2024-05-15"))
    .select(
        "*",
        F.when(F.col("job_title").isin(["Designer", "Manager"]), "Group A").otherwise("Other").alias("job_title_group"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    latest
    .filter(F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat("full_name", F.lit(" - "), "job_title").alias("full_name_job_title"),
        F.coalesce("level", F.lit("OTHER")).alias("level_filled"),
    )
)

enriched = (
    valid
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w2))
    .withColumn("salary_avg_3", F.avg("salary").over(w3))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.rank().over(w6).alias("salary_avg_3_rank"),
        F.first("level", ignorenulls=True).over(w7).alias("first_known_level"),
    )
)

enriched_employees = (
    scored
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.location"), F.col("d.dept_name"))
)

result = (
    enriched_employees
    .groupBy("dept_id")
    .pivot("job_title", ["Intern", "Engineer"])
    .agg(F.sum("event_seq").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
