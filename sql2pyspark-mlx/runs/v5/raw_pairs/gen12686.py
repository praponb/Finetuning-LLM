from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("email").isNotNull() & (F.col("age") <= 49)) | F.col("is_active"))
    .select(
        "credit_limit",
        "signup_date",
        F.concat_ws("/", "segment", "email").alias("segment_email_key"),
    )
)
