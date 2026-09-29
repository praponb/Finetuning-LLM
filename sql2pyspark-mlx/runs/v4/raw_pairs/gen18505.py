from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "dept_name",
        "budget",
        F.when(F.col("dept_name").isin(["Marketing", "HR"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
)
