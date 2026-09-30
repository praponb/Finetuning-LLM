from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "opened_date",
        F.when(F.col("region") == "West", 1).otherwise(0).alias("is_west"),
    )
)
