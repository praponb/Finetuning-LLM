from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.lower(F.trim("name")) == "david lee") & (F.lower(F.trim("email")) == "henry@startup.io"))
)

signup_quarter_summary = (
    base
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
    )
)

result = (
    signup_quarter_summary
    .filter(F.col("pct_corporate") > 64)
    .orderBy(F.col("pct_corporate").desc(), "signup_quarter")
    .limit(50)
)
