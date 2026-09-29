from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
)
