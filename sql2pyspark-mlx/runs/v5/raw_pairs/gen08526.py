from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "id")
    .intersectAll(
        df_departments
        .filter(F.col("location").isin(["Berlin", "New York"]))
        .select("dept_name", "id")
    )
)
