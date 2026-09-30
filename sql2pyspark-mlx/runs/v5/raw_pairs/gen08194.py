from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("id")
    .intersectAll(
        df_customers
        .filter(F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
        .select("id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 61, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_month", "age_tier"], ["activity_month"]],
        "activity_month",
        "age_tier",
    )
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.bit_or("age").alias("age_bit_or"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
    )
)
