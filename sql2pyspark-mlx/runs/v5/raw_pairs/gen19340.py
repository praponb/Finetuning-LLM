from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "email",
        F.when(F.col("level") == "L1", 1).otherwise(0).alias("is_l1"),
    )
)
