from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") <= 2.1)
        .select("country", "city")
    )
)
