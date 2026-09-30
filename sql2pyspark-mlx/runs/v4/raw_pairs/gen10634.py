from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@startup.io"))
    .select("level", "email")
    .unionByName(
        df_employees
        .filter(F.col("manager_id") == 31)
        .select("level", "email")
    )
    .distinct()
    .orderBy(F.col("level").desc())
)
