from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").between(10000, 10800))
        .select("id", "segment")
    )
)
