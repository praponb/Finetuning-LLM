from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.lower(F.trim("email")) == "carol@company.co") & (F.col("signup_date") <= "2024-05-01"))
    .select(
        "id",
        "city",
        "segment",
        "name",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
        F.when(F.col("credit_limit") >= 14000, "High").when(F.col("credit_limit") >= 7000, "Medium").otherwise("Low").alias("credit_limit_tier"),
    )
    .orderBy(F.col("segment").desc(), "id")
    .limit(10)
)
