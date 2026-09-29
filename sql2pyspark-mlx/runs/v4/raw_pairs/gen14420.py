from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name")
    .exceptAll(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("id", "dept_name")
    )
)
