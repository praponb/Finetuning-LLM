from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-05"))) & F.col("city").isin(["Bangkok", "Berlin", "Chiang Mai"]))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
    .withColumn("store_name_prefix", F.substring("store_name", 1, 3))
)

result = (
    valid
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("city") != "Tokyo")
        .select("id", "region")
    )
)
