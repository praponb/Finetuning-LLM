from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Consumer"]))
    .select("id", "age", "country")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit").between(12200, 12300))
        .select("id", "age", "country")
    )
    .distinct()
)
