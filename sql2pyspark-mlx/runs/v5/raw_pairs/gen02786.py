from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@company.co"))
    .select(F.col("hire_date").alias("event_date"))
    .unionByName(
        df_customers
        .filter(F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
        .select(F.col("signup_date").alias("event_date"))
    )
    .distinct()
    .orderBy("event_date")
)
