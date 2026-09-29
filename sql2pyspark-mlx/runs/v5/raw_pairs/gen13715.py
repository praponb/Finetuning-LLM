from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "SMB") & F.col("city").isNotNull() & (F.col("credit_limit") >= 15100))
    .select(
        "id",
        "signup_date",
        "segment",
        F.when(F.col("age") >= 56, "High").when(F.col("age") >= 25, "Medium").otherwise("Low").alias("age_level"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
