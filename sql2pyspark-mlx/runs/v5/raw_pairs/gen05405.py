from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("start_date")
    )
)
