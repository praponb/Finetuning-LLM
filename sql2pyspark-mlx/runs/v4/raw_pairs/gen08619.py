from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-10-01").cast("date")) & (F.col("city") == "London"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

result = (
    valid
    .select("region", "id")
    .intersect(
        df_stores
        .filter(F.col("city") == "Chiang Mai")
        .select("region", "id")
    )
)
