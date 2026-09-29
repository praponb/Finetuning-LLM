from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com"))
    .select("signup_date")
    .subtract(
        df_customers
        .filter(F.month("signup_date").isin([1, 4, 10]))
        .select("signup_date")
    )
)
