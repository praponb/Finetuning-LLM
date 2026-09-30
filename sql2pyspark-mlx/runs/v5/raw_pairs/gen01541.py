from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3786000) & F.col("dept_name").isNull() & (F.col("budget") < 2440000))
    .groupBy("dept_name", "location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy("dept_name_values", "dept_name")
)
