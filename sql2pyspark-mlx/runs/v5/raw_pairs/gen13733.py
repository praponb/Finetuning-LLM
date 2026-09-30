from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select("status", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("amount") >= 910)
        .select("status", "customer_id")
    )
    .distinct()
)
