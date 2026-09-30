from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("email").isNotNull())
    .select("id", "email")
)
