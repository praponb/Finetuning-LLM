from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "segment")
    .intersect(
        df_customers
        .filter(F.col("city").isin(["Bangkok", "New York", "Tokyo"]))
        .select("country", "segment")
    )
)
