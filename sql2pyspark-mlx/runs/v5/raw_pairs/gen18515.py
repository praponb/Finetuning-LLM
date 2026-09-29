from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Sales", "HR"]) | (F.col("budget") <= 2415000))
    .select("location", "budget")
    .orderBy("budget", "location")
)
