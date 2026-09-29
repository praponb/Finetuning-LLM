from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") > "2023-03-01")
    .groupBy("is_remote")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .filter(F.col("l3_salary") >= 24)
    .drop("l3_salary")
)
