from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 39) & (F.col("hire_date") <= "2024-10-01") & (F.col("salary") >= 110000))
    .select("id", "is_remote", "full_name")
    .orderBy("id")
)
