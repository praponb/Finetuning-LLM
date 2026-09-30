from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 5086) & (F.col("region") == "East"))
    .select(
        "id",
        "store_name",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
    .orderBy(F.col("store_name").desc(), F.col("days_since_opened_date").desc())
)
