from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1993000)
    .select(
        "budget",
        F.when(F.col("dept_name").isin(["HR", "Engineering"]), "Group A").otherwise("Other").alias("dept_name_group"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)
