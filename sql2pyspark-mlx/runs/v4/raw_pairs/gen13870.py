from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 8300)
    .select("id", "email")
    .subtract(
        df_customers
        .filter(F.col("country").isin(["TH", "SG"]))
        .select("id", "email")
    )
)
