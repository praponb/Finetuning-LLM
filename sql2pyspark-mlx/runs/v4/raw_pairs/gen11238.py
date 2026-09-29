from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Legal", "Engineering"]))
    .select("id", "location", "dept_name", "budget")
)
