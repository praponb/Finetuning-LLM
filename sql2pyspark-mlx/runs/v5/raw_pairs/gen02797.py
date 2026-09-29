from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Director", "Engineer"]) & ~F.col("level").isin(["L2", "L1", "L5"]))
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .filter(F.col("l3_salary") >= 37)
    .orderBy(F.col("l3_salary").desc(), "is_remote")
    .limit(5)
)
