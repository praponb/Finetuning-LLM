from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("supplier_name").like("T%"))

summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Chiang Mai"]), "group_a").otherwise("other").alias("city_group"),
        "city",
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)

result = (
    summary
    .filter(F.col("stddev_rating") > 65)
    .select("city_group", "city", "stddev_rating")
)
