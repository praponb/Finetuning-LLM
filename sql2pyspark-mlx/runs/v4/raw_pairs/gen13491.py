from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("legal_budget") < 33)
    .select("location", "dept_name", "legal_budget", "location_values")
    .orderBy("dept_name")
)
