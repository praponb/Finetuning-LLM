from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["London", "Berlin", "New York"]))
    .select(
        "id",
        "dept_name",
        F.when(F.col("dept_name").isin(["Legal", "Engineering"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
)
