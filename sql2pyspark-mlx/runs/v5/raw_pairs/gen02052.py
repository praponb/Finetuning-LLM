from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("He%"))
    .select("signup_date", "segment")
    .intersect(
        df_customers
        .filter(F.col("credit_limit").between(8800, 9100))
        .select("signup_date", "segment")
    )
    .orderBy(F.col("signup_date").desc())
)
