from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNull())
    .select("id", "city", "segment")
    .unionByName(
        df_customers
        .filter(F.col("is_active"))
        .select("id", "city", "segment")
    )
    .distinct()
)
