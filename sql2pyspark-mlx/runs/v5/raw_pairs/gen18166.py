from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.concat_ws(",", "city", "name").alias("city_name_key"),
        F.substring("name", 1, 3).alias("name_prefix"),
    )
)

result = (
    base
    .groupBy("segment")
    .pivot("country", ["UK", "US"])
    .agg(F.sum("credit_limit").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "segment",
        F.col("UK_total").alias("uk_total"),
        F.col("UK_cnt").alias("uk_cnt"),
        F.col("US_total").alias("us_total"),
        F.col("US_cnt").alias("us_cnt"),
    )
)
