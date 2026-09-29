from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("name").isNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.concat_ws("-", "segment", "email").alias("segment_email_key"),
    )
)

result = (
    cleaned
    .select("segment", "country")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") > 5600)
        .select("segment", "country")
    )
)
