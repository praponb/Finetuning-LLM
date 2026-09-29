from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["UK", "DE"]))
        .select("city", "id")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.7, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
    )
    .orderBy(F.col("tokyo_events").desc_nulls_last())
)
