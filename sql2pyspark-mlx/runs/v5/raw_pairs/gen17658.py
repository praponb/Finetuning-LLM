from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 93000) & (F.col("email").isNotNull() | F.col("email").isNull()))
    .select("id", "full_name", "email")
)
