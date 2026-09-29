from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai", "London"]) & F.col("dept_name").isin(["Legal", "HR"]))
    .select("id", "dept_name", "budget")
)
