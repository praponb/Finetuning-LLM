from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & F.col("dept_name").isin(["HR", "Marketing", "Legal"]))
    .select("dept_name", "budget", "location")
    .orderBy(F.col("dept_name").desc())
)
