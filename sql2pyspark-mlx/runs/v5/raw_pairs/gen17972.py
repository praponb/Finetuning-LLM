from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Kim%"))
    .select("id", "age", "name")
    .unionByName(
        df_customers
        .filter(F.col("city") == "Chiang Mai")
        .select("id", "age", "name")
    )
    .distinct()
)
