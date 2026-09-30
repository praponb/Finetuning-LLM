from pyspark.sql import functions as F

result = (
    df_shipments
    .select("carrier", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("carrier") != "UPS")
        .select("carrier", "order_id")
    )
)
