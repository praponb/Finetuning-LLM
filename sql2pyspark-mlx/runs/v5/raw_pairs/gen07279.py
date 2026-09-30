from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "RETURNED"]))
    .select("id", "delivered_date")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") <= "2024-01-02")
        .select("id", "delivered_date")
    )
    .orderBy("delivered_date")
)
