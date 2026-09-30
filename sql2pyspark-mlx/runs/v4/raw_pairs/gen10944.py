from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2138000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") > 2304000)
        .select("dept_name", "location")
    )
    .distinct()
    .orderBy(F.col("location").desc())
)
