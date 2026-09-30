from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "level",
        "manager_id",
        "full_name",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
    )
    .orderBy(F.col("manager_id").desc())
)
