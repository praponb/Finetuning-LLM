from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2024-03-25").cast("date")) & F.col("city").isNotNull() & F.col("name").like("No%"))
    .withColumn("country_group", F.when(F.col("country").isin(["DE", "US"]), "Tier 1").otherwise("Other"))
    .withColumn("city_segment", F.concat("city", F.lit(" "), "segment"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("DIRECT")))
)

result = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isNotNull()), "left_anti")
)
