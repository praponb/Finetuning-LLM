from pyspark.sql import functions as F

result = (
    df_shipments
    .select("order_id", "status")
    .intersect(
        df_shipments
        .filter(F.year("delivered_date") == 2023)
        .select("order_id", "status")
    )
)
