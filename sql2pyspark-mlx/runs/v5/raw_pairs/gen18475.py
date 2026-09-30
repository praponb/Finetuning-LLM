from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") <= 75000) & F.col("full_name").like("%Che%"))
    .select(
        "manager_id",
        "email",
        F.regexp_replace("email", " ", "").alias("email_slug"),
    )
    .orderBy("email_slug")
)
