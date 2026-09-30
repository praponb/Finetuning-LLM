from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai", "Singapore"]) & F.col("location").isin(["Singapore", "Bangkok", "Tokyo"]))
    .groupBy("dept_name")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy(F.col("dept_name_values").desc(), "dept_name")
    .limit(50)
)
