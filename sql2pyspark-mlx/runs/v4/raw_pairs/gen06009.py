from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"))
    .subtract(
        df_customers
        .filter(F.col("city").isin(["Singapore", "Tokyo"]))
        .select(F.col("signup_date").alias("event_date"))
    )
    .orderBy("event_date")
)
