from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select("channel", "store_id")
    .intersect(
        df_orders
        .filter(~F.col("channel").isin(["store", "partner"]))
        .select("channel", "store_id")
    )
)
