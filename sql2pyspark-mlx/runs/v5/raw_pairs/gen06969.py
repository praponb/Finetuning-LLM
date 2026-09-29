from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(
        df_stores
        .filter(~F.col("city").isin(["London", "Chiang Mai"]))
        .select("opened_date")
    )
    .distinct()
)
