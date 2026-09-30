from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 40)
    .select(
        "id",
        "discount",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.when(F.col("status").isin(["SHIPPED", "RETURNED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)
