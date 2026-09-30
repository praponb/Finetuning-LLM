from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount") < 0.23)
    .select(
        "id",
        "order_date",
        "discount",
        F.lead("discount", 2).over(w).alias("next_discount"),
    )
    .orderBy("order_date")
)
