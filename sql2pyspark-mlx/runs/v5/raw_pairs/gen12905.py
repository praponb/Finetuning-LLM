from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        F.percent_rank().over(w).alias("shipped_date_pct_rank"),
    )
    .orderBy(F.col("shipped_date").desc(), "order_id")
)
