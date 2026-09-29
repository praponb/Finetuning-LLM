from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Lee%"))
    .select("id", "hire_date", "level")
    .unionByName(
        df_employees
        .filter(F.col("salary") <= 74000)
        .select("id", "hire_date", "level")
    )
    .orderBy(F.col("id").desc())
)
