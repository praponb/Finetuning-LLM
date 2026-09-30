from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "UPS", "Flash"]) | F.col("delivered_date").between("2024-10-28", "2024-11-29"))
    .select("id", "delivered_date", "order_id")
    .orderBy("order_id")
)
