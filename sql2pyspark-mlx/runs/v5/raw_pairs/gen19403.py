from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

result = (
    flattened
    .select(
        "customer_id",
        "created_at",
        "price",
        "promo",
        F.avg("price").over(w_running).alias("price_avg_15m"),
    )
)
