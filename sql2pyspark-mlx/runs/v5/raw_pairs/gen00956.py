from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(
        df_subscriptions
        .filter(F.col("status") == "EXPIRED")
        .select("start_date")
    )
    .distinct()
    .orderBy(F.col("hire_date").desc())
)
