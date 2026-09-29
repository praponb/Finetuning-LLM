from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") != "SMB") & F.col("signup_date").between("2025-03-01", "2025-06-01"))
    .select(
        "is_active",
        "signup_date",
        "segment",
        F.concat_ws(",", "country", "segment").alias("country_segment_key"),
    )
)
