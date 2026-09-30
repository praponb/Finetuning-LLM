from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "New York", "Tokyo"]))
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["tablet", "desktop"]))
        .select("country")
    )
    .distinct()
    .orderBy(F.col("country").desc())
)
