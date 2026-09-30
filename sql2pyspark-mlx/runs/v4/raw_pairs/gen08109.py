from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy(F.col("salary_range").desc(), F.col("dept_id").desc())
)
