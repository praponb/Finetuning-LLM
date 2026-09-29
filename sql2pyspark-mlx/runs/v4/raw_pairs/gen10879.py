from pyspark.sql import functions as F

src = df_employees.filter(F.col("dept_id").isin([2, 4, 6, 18]))

level_summary = (
    src
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
)

result = (
    level_summary
    .filter(F.col("l4_count") > 3)
    .select("level", "l4_count")
)
