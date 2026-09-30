from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "South") & F.col("city").isin(["Singapore", "Chiang Mai", "Bangkok"]))
    .select(
        "id",
        "store_name",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)
