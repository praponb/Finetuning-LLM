from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("dept_name").isNotNull() | F.col("dept_name").isin(["Engineering", "Legal"]))
)

agg = (
    recent_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = (
    agg
    .select("location", "location_values")
    .orderBy("location_values")
)
