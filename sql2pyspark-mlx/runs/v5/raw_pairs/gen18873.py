from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("num_employees"),
    )
    .filter(F.col("num_employees") >= 5)
    .orderBy(F.col("hire_month").desc())
)
