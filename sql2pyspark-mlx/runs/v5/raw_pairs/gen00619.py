from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("country") == "JP") & (F.upper(F.trim("email")) == "FRANK@COMPANY.CO")))
    .select(
        "id",
        "city",
        "country",
        "age",
        F.date_add("signup_date", 84).alias("signup_date_plus_84d"),
        F.lower("segment").alias("segment_lower"),
    )
)
