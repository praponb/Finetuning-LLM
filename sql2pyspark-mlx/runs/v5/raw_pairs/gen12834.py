from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.count("*").alias("cnt"))
)
