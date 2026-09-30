from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("dept_id", "level")
    .orderBy("dept_id")
)
