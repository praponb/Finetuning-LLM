from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w))
    .withColumn("running_salary", F.sum("salary").over(w_grp))
)

result = (
    enriched
    .cube(
        F.when(F.col("salary") >= 120000, "LARGE").otherwise("SMALL").alias("salary_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.when(F.col("running_salary") >= 620647, "LARGE").otherwise("SMALL").alias("running_salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.sum("running_salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("running_salary_per_intern"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list_2"),
    )
    .orderBy("quarter_start")
)
