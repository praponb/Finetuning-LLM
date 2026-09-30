from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["New York", "Bangkok", "Singapore"]))
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)
