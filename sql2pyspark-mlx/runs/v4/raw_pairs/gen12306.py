from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(840, 1090))
    .select("status", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("status") != "RETURNED")
        .select("status", "order_date")
    )
    .orderBy("order_date")
)
