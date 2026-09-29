from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("name")) == "JACK TAYLOR")
    .select(
        "id",
        "email",
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.when(F.col("segment").isin(["SMB", "Consumer"]), "Core").otherwise("Other").alias("segment_group"),
    )
    .orderBy(F.col("id").desc(), F.col("segment_group").desc())
)
