from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("credit_limit").isNull() | F.col("country").isin(["DE", "US"])))
    .select(
        "id",
        "signup_date",
        "city",
        "segment",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.initcap(F.trim("name")).alias("name_title"),
    )
)
