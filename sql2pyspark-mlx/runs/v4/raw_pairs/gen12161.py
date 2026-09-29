from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id")

result = (
    df_orders
    .filter(F.col("shipping_fee").between(35, 38))
    .select(
        "id",
        "store_id",
        "customer_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
    .orderBy(F.col("amount").desc())
)
