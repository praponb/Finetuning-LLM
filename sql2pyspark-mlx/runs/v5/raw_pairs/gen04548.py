from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .subtract(
        df_subscriptions
        .filter(F.year("end_date") == 2023)
        .select(F.col("end_date").alias("launch_date"))
    )
    .orderBy(F.col("launch_date").desc())
)
