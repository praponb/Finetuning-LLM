from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Chiang Mai", "London", "Tokyo"]) & (F.col("country") != "UK"))
    .select(
        "id",
        "rating",
        "city",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
    .orderBy("city")
)
