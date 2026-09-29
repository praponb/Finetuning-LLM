from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("id", "segment")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit").between(8800, 10000))
        .select("id", "segment")
    )
    .distinct()
)
