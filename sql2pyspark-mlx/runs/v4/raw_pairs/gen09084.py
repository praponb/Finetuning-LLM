from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select("city")
    .union(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("location")
    )
    .distinct()
)
