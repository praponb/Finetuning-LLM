from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("city") == "Tokyo"))
    .withColumn("clean_segment", F.trim("segment"))
)

result = (
    valid
    .select("segment", "city")
    .intersect(
        df_customers
        .filter(F.lower(F.trim("name")) == "olivia garcia")
        .select("segment", "city")
    )
)
