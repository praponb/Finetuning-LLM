from pyspark.sql import functions as F

result = (
    df_employees
    .select("is_remote", "manager_id", F.length("full_name").alias("full_name_length"))
)
