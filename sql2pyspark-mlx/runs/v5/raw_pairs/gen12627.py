from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter((F.col("salary") > 62000) & ((F.col("salary") <= 113000) | (F.col("hire_date") <= "2024-10-29")))
)

is_remote_summary = (
    valid_employees
    .groupBy("is_remote")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    is_remote_summary
    .select("is_remote", "last_hire_date")
    .orderBy("last_hire_date", "is_remote")
    .limit(5)
)
