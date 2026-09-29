from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2024-08-19")
    .select(
        "id",
        "hire_date",
        F.when(F.col("level") == "L2", 1).otherwise(0).alias("is_l2"),
        F.concat_ws("-", "email", "full_name").alias("email_full_name_key"),
    )
)
