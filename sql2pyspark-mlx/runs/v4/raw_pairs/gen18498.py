from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 82000)
    .select("level", "dept_id")
    .unionByName(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("level", "dept_id")
    )
    .distinct()
)
