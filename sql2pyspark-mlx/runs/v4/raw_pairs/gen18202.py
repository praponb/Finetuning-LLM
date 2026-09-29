from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("budget") <= 1605000)
        .select("location", "dept_name")
    )
)
