from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-06-08", "2024-12-17"))
    .select("country", "email")
    .unionByName(
        df_customers
        .filter(~F.col("country").isin(["UK", "TH", "JP"]))
        .select("country", "email")
    )
    .orderBy(F.col("country").desc())
)
