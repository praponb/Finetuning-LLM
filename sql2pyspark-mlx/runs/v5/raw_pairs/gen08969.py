from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Chiang Mai", "Singapore"]) & (F.col("city") == "Singapore"))
    .select(
        "opened_date",
        "store_name",
        "city",
        F.upper("store_name").alias("store_name_upper"),
    )
)
