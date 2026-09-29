from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([14, 33, 35, 36]))
    .select("id", "full_name", "level")
    .unionByName(
        df_employees
        .filter(F.year("hire_date") == 2025)
        .select("id", "full_name", "level")
    )
    .orderBy("level")
)
