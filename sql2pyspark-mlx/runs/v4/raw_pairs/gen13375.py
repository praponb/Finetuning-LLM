from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR"]))
    .groupBy(
        F.when(F.col("budget") >= 3788000, "high").when(F.col("budget") >= 2027000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)
