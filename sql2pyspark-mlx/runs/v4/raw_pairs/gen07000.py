from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "Consumer"]))
    .select("id", "email", "segment")
    .unionByName(
        df_customers
        .filter(F.col("city") == "Singapore")
        .select("id", "email", "segment")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
