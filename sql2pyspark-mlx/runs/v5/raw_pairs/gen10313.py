from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "city",
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
    )
)
