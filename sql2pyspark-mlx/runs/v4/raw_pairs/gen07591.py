from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "is_active",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
        F.greatest("age", "credit_limit").alias("greatest_age_credit_limit"),
    )
    .orderBy("name")
)
