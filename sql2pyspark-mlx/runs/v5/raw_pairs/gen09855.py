from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") >= 4500) & F.col("credit_limit").between(4800, 15500))
    .select(
        "signup_date",
        "is_active",
        "name",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
    .orderBy(F.col("signup_date").desc(), F.col("name").desc())
)
