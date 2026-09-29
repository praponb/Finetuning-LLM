from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select(
        "*",
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

result = (
    filtered
    .select("city")
    .intersectAll(
        df_suppliers
        .filter(F.col("city").isin(["Chiang Mai", "Singapore"]))
        .select("city")
    )
)
