from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 34)
    .select("id", "country", "signup_date")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit") < 13000)
        .select("id", "country", "signup_date")
    )
)
