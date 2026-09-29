from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select(
        "*",
        F.lower("name").alias("name_lower"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

result = (
    valid
    .select("country", "id")
    .intersect(
        df_customers
        .filter(F.col("name").like("Gr%"))
        .select("country", "id")
    )
)
