from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("hire_date") > "2025-06-28") & (F.col("manager_id") == 19))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "level",
    )
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
    .orderBy(F.col("level").desc(), "hire_cohort")
)
