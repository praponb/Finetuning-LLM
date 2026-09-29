from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["Central", "South", "East"]))
        .select("city")
    )
    .orderBy("city")
)
