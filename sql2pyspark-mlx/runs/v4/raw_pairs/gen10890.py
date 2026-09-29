from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Marketing"]))
    .withColumn("location_group", F.when(F.col("location").isin(["Bangkok", "Berlin"]), "Priority").otherwise("Other"))
)

agg = (
    valid_departments
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = (
    agg
    .filter(F.col("unique_dept_names") >= 4)
    .orderBy("dept_name")
)
