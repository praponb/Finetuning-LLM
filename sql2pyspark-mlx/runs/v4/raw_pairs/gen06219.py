from pyspark.sql import functions as F

result = (
    df_shipments
    .select("carrier", "status")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date") >= "2024-01-01")
        .select("carrier", "status")
    )
)
