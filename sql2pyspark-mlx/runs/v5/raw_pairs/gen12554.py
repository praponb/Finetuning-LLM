from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 67000)
    .select("hire_date")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("hire_date")
    )
    .distinct()
)
