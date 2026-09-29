from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select("id", "delivered_date", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") < "2024-11-01")
        .select("id", "delivered_date", "shipped_date")
    )
)
