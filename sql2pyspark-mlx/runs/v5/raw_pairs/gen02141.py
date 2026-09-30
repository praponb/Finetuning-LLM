from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1476000)
    .groupBy(
        F.when(F.col("dept_name").isin(["HR", "Legal"]), "priority").otherwise("other").alias("dept_name_group"),
        F.when(F.col("budget") >= 3428000, "high").when(F.col("budget") >= 1760000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .filter(F.col("unique_dept_names") > 4)
    .drop("unique_dept_names")
    .orderBy(F.col("total_budget").desc())
)
