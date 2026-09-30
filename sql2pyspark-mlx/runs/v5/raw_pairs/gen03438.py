from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

result = (
    cleaned
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("city").isin(["Berlin", "Singapore"]))
        .select("country")
    )
)
