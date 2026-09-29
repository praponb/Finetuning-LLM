from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") >= 2024)
    .select("signup_date")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit") < 12200)
        .select("signup_date")
    )
    .orderBy("signup_date")
)
