from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & ~F.col("name").like("%Ford%"))
    .groupBy("city")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)
