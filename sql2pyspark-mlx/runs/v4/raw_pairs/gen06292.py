from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@mail.co.th") & F.col("email").like("%@mail.co.th") & F.col("email").isNotNull())
    .select("id", "signup_date")
    .orderBy(F.col("signup_date").desc())
)
