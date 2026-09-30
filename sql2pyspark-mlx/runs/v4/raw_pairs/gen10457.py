from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["CANCELLED", "COMPLETED"]))
    .select("customer_id")
    .unionByName(
        df_orders
        .filter(F.col("amount").between(1010, 1570))
        .select("customer_id")
    )
    .distinct()
    .orderBy("customer_id")
)
