from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"))
    .intersect(
        df_customers
        .filter(F.col("segment") == "SMB")
        .select(F.col("signup_date").alias("event_date"))
    )
    .orderBy(F.col("event_date").desc())
)
