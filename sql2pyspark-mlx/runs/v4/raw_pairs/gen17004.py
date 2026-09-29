from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2612000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") >= 1172000)
        .select("dept_name", "location")
    )
    .distinct()
)
