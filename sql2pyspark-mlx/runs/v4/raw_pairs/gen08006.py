from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3589000)
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("location") != "Chiang Mai")
        .select("location", "dept_name")
    )
    .orderBy(F.col("dept_name").desc())
)
