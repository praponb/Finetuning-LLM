from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2025-04-01")
    .select("id", "delivered_date", "status")
    .intersect(
        df_shipments
        .filter(F.month("shipped_date").isin([1, 6, 12]))
        .select("id", "delivered_date", "status")
    )
)
