from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "Bangkok")
        .select("country", "city")
    )
)
