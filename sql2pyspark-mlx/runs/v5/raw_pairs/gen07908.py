from pyspark.sql import functions as F

result = (
    df_departments
    .cube(
        "dept_name",
        F.when(F.col("dept_name").isin(["Legal", "Engineering"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy(F.col("dept_name").desc())
)
