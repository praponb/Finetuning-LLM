from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
    )
    .orderBy(F.col("is_consumer").desc())
)
