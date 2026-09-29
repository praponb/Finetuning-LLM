from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "US", "JP"]))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.when(F.col("city").isin(["Bangkok", "London"]), "Core").otherwise("Other").alias("city_group"),
    )
)

result = (
    valid
    .groupBy("city")
    .pivot("country", ["DE", "US", "SG"])
    .agg(F.avg("rating").alias("value_sum"))
)
