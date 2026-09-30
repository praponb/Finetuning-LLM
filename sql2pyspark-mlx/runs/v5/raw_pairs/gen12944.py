from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-07-30").cast("date"))
    .withColumn("clean_name", F.trim("name"))
    .withColumn("city_lower", F.lower("city"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    cleaned
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .groupingSets(
        [["activity_month", "country", "quarter_start"], ["activity_month", "country"], []],
        "activity_month",
        "country",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
    )
    .filter(F.col("tokyo_rows") > 1)
)
