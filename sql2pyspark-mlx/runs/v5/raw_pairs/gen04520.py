from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country").isin(["TH", "SG"]) & (F.col("country") == "JP")) | F.col("city").isNotNull())
    .groupBy(
        "city",
        F.when(F.col("country").isin(["SG", "UK"]), "tier_1").otherwise("other").alias("country_group"),
        "country",
    )
    .agg(F.countDistinct("rating").alias("unique_citys"))
    .orderBy("unique_citys")
)
