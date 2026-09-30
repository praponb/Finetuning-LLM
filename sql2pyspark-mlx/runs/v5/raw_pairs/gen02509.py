from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "credit_limit",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.initcap(F.trim("email")).alias("email_title"),
    )
)
