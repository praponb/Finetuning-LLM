from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["DE", "TH", "UK"]))
    .select("signup_date", "age")
    .unionByName(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("signup_date", "age")
    )
    .distinct()
)
