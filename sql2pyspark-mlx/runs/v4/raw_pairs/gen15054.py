from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L2", "L1"]) & (F.col("salary") > 138000))
    .groupBy("manager_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.min("hire_date").alias("first_hire_date"),
    )
)
