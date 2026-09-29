from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([4, 6, 7, 8]))
        .select("last_restock")
    )
    .orderBy("start_date")
)
