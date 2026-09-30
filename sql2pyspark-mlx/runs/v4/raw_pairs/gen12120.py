from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-06-26", "2024-10-01") & F.col("region").isin(["East", "North"]))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.length("city").alias("city_length"),
        F.upper("region").alias("region_upper"),
    )
)
