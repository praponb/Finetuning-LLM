from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy(F.col("hire_quarter").desc())
)
