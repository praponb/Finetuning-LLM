from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
    )
)
