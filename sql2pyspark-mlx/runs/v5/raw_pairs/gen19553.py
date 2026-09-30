from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.month("signup_date").isin([1, 4, 6]) & F.col("age").between(41, 43)) | ~F.col("is_active"))
    .select(
        "id",
        "signup_date",
        "age",
        "country",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
    )
)
