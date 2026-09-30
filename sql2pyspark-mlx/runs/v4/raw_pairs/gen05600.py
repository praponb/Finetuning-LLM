from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "Finance", "HR"]))
        .select("dept_name", "location")
    )
)
