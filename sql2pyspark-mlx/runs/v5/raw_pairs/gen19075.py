from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy(F.col("hire_quarter").desc())
)
