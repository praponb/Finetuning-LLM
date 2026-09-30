from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("Berlin")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.ceil("rating").alias("rating_whole"),
    )
)

result = (
    base
    .groupBy(
        "city",
        F.when(F.col("rating_whole") >= 70, "HIGH").otherwise("LOW").alias("rating_whole_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("chiang_mai_events").desc())
)
