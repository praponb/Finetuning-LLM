from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-03-16")
    .select(
        "id",
        "shipped_date",
        "cost",
        (F.col("cost") - F.lag("cost", 2).over(w)).alias("cost_change"),
    )
)
