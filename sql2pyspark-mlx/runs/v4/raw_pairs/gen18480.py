from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-10-01", "2024-11-01") & (F.col("manager_id") == 21) & F.col("is_remote"))
    .select(
        "id",
        "salary",
        "is_remote",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
    )
)
