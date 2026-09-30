from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED"]) & (F.col("status") != "RETURNED"))
    .select(
        "id",
        "status",
        "order_id",
        "delivered_date",
        F.col("cost").cast("int").alias("cost_int"),
        F.lower("status").alias("status_lower"),
    )
)
