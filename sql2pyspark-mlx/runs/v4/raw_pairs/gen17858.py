from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "opened_date",
        F.when(F.col("city") == "London", 1).otherwise(0).alias("is_london"),
    )
)
