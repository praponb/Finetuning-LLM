from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL"]))
    .select("id", "order_id", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.month("delivered_date").isin([1, 2, 6]))
        .select("id", "order_id", "delivered_date")
    )
)
