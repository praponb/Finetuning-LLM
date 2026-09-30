from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & ~F.col("city").isin(["Chiang Mai", "New York"]))
    .select("*", F.floor("rating").alias("rating_whole"))
)

result = (
    cleaned
    .select("city")
    .subtract(
        df_suppliers
        .filter(F.col("city") == "Chiang Mai")
        .select("city")
    )
)
