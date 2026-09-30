from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "name",
        "signup_date",
        "credit_limit",
        F.when(F.col("city").isin(["Bangkok", "New York"]), "Group A").otherwise("Other").alias("city_group"),
    )
)
