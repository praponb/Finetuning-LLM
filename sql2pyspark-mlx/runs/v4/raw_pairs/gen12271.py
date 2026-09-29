from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-09-01").cast("date")) & F.col("region").isNotNull())
    .select("*", F.lower("city").alias("city_lower"))
)

result = (
    base
    .select("id", "region")
    .intersectAll(
        df_stores
        .filter(F.col("city").isin(["New York", "Bangkok"]))
        .select("id", "region")
    )
)
