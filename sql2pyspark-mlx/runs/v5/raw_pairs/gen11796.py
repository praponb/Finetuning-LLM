from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "Corporate", "SMB"]) | F.col("name").isNull())
    .select(
        "signup_date",
        F.lower("city").alias("city_lower"),
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
    )
)
