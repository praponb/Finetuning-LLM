from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("city") == "Chiang Mai")
        .select("city", "id")
    )
)
