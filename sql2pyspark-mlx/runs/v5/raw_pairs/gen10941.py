from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2024)
    .select("id", "order_id")
    .unionByName(
        df_shipments
        .filter(~F.col("carrier").isin(["UPS", "Flash"]))
        .select("id", "order_id")
    )
    .distinct()
)
