from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Chiang Mai", "London"]) & (F.col("budget") >= 3408000))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy(F.col("dept_name_values").desc())
)
