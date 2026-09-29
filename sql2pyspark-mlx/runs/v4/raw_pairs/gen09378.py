from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_employees
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w_hist))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
)

result = (
    scored
    .rollup("dept_id", "level")
    .agg(
        F.percentile_approx("salary_avg_5_prior", [0.25, 0.5, 0.75]).alias("salary_avg_5_prior_quartiles"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("salary_p90") >= 0)
    .orderBy(F.col("dept_id").desc())
)
