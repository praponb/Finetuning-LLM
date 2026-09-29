from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_sales_by_store.alias("StoreSalesByStore"), F.col("stores.id") == F.col("StoreSalesByStore.store_id"), "left")
    .select(
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.col("StoreSalesByStore.revenue_range"),
        F.col("StoreSalesByStore.avg_units"),
    )
)
