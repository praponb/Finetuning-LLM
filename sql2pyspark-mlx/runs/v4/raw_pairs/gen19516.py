from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("age", "email")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit") >= 8600)
        .select("age", "email")
    )
    .distinct()
)
