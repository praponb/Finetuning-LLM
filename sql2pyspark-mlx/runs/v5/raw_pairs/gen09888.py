from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("country").isin(["UK", "SG"]))

customers_stats = (
    filtered_customers
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.countDistinct("age").alias("unique_countrys"),
    )
)

result = customers_stats
