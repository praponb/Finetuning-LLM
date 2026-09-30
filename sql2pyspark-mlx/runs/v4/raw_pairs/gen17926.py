from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["South", "East", "Central"]))
        .select("city")
    )
)
