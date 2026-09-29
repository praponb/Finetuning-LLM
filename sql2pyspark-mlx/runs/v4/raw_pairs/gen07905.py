from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .intersect(
        df_products
        .filter(F.array_contains("tags", "eco"))
        .select("launch_date")
    )
    .orderBy("order_date")
)
