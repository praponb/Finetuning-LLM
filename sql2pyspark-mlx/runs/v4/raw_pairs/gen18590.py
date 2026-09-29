from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.length("email") > 7)
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.total_count") >= 9)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("D.total_count"),
        F.col("D.level_list"),
    )
    .orderBy("total_count")
)
