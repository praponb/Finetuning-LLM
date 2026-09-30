from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select("delivered_date", "order_id")
    .unionByName(
        df_shipments
        .filter(F.year("delivered_date") >= 2024)
        .select("delivered_date", "order_id")
    )
)
