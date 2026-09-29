from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "HR", "Engineering"]) | F.col("budget").isNull())
    .groupBy(
        F.when(F.col("budget") >= 3416000, "high").when(F.col("budget") >= 1805000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("dept_name").isin(["Finance", "Legal"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.count("*").alias("num_departments"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .filter(F.col("num_departments") > 3)
)
