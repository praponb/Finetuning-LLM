from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("age")
    .unionByName(
        df_customers
        .filter(F.col("signup_date") >= "2024-12-01")
        .select("age")
    )
    .distinct()
    .orderBy("age")
)
