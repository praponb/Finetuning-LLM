from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-07-01")
    .select("id", "status")
    .unionByName(
        df_orders
        .filter(F.col("status") == "COMPLETED")
        .select("id", "status")
    )
    .orderBy("id")
)
