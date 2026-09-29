from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & F.col("opened_date").between("2024-08-28", "2024-12-01"))
    .select(
        "id",
        "opened_date",
        "city",
        "store_name",
        F.lower("store_name").alias("store_name_lower"),
        F.col("sqft").cast("double").alias("sqft_num"),
    )
)
