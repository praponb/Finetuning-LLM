from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select("id", "manager_id", "email")
    .unionByName(
        df_employees
        .filter(F.col("manager_id") == 22)
        .select("id", "manager_id", "email")
    )
    .orderBy("manager_id")
)
