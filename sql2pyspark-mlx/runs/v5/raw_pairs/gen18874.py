from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("location") == "Tokyo")
        .select("dept_name")
    )
)
