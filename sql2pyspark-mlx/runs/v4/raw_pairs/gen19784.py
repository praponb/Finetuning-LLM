from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-03-01")
    .select("order_id")
    .unionByName(
        df_shipments
        .filter(F.col("status") == "IN_TRANSIT")
        .select("order_id")
    )
    .distinct()
)
