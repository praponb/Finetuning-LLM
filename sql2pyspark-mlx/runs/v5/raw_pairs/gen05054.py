from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5", "L4"]))
    .groupBy("is_remote")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .filter(F.col("stddev_salary") > 55)
    .orderBy("stddev_salary", "is_remote")
    .limit(1)
)
