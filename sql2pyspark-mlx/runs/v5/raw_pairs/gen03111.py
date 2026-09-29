from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(1.9, 3.2))
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["UK", "SG", "JP"]))
        .select("country", "city")
    )
)
