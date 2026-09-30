from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 60)
    .select("email")
    .unionByName(
        df_customers
        .filter(F.lower(F.trim("email")) == "carol@company.co")
        .select("email")
    )
    .distinct()
)
