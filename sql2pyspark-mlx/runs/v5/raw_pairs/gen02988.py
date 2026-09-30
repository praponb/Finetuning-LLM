from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") < 11)
    .select(
        "signup_date",
        "age",
        "city",
        F.lower("name").alias("name_lower"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
