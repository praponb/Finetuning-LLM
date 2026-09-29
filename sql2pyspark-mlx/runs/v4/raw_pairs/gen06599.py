from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Bangkok")
    .groupBy(
        F.when(F.col("dept_name").isin(["Legal", "Sales"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") < 33)
    .select("dept_name_group", "budget_range")
    .orderBy(F.col("dept_name_group").desc())
)
