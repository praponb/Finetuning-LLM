from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-07-01")
    .select(
        "id",
        "store_name",
        F.col("sqft").cast("string").alias("sqft_num"),
        F.date_add("opened_date", 2).alias("opened_date_plus_2d"),
    )
)
