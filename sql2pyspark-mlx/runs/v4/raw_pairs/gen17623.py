from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("budget") < 2852000)
        .select("dept_name")
    )
)
