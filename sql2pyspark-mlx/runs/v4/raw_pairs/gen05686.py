from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com") & F.col("signup_date").between("2023-07-08", "2024-06-20") & (F.col("city") == "London"))
    .select(
        "id",
        "city",
        "is_active",
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
    )
    .orderBy(F.col("id").desc(), F.col("city").desc())
)
