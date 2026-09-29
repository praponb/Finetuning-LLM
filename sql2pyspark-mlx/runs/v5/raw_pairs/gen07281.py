from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

latest = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .withColumn("salary_tier", F.when(F.col("salary") >= 129000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["salary_tier", "dept_id"], ["salary_tier"]],
        "salary_tier",
        "dept_id",
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Director", 1)), F.lit(0)), 2).alias("salary_per_director"),
    )
)
