from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("delivered_date")
    )
    .orderBy("launch_date")
)
