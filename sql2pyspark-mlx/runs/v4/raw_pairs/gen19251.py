from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(((F.upper(F.trim("email")) == "DAVID.LEE@GMAIL.COM") & ~F.col("is_active")) | F.col("email").isNotNull())
    .withColumn("age_whole", F.ceil("age"))
)

agg = (
    recent_customers
    .groupBy("city", "country")
    .agg(F.min("signup_date").alias("min_signup_date"))
)

result = (
    agg
    .select("city", "country", "min_signup_date")
    .orderBy("min_signup_date", F.col("country").desc())
)
