from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2023-03-01", "2025-10-12"))
        .select("status")
    )
)
