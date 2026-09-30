from pyspark.sql import functions as F

result = (
    df_shipments
    .select("order_id", "status")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") <= "2025-06-01")
        .select("order_id", "status")
    )
)
