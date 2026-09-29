from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "KANYA@MAIL.CO.TH")
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "segment",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)
