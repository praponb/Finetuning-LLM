from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("country").isin(["UK", "JP"]))
        .select("country", "id")
    )
)
