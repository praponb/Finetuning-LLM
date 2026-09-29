from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.substring("country", 1, 4).alias("country_prefix"),
        F.when(F.col("city").isin(["Bangkok", "Singapore"]), "Group A").otherwise("Other").alias("city_group"),
    )
    .orderBy("city", "country_prefix", "id")
    .limit(5)
)
