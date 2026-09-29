from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Legal"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .orderBy("location_values", F.col("dept_name_group").desc())
)
