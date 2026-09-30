from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

deduped = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "row_num",
    )
    .orderBy("shipping_fee")
)
