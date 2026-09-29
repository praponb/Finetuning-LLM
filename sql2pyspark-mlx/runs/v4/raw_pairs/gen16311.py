from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "UK"]))
    .select("email", "signup_date")
    .unionByName(
        df_customers
        .filter(F.col("country") != "TH")
        .select("email", "signup_date")
    )
    .distinct()
    .orderBy(F.col("email").desc())
)
