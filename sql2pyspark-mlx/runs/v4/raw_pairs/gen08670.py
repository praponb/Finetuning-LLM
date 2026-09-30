from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date") > "2023-06-01")
        .select("status")
    )
)
