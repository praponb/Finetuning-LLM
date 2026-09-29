from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 130000)
    .select("email")
    .subtract(df_employees.filter(~F.col("is_remote")).select("email"))
    .orderBy(F.col("email").desc())
)
