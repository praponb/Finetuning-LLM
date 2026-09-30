from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") >= "2025-04-25")
        .select("shipped_date")
    )
)
