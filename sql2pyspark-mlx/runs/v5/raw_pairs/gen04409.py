from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("cost") > 81)
    .select(
        "id",
        "delivered_date",
        "cost",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
    )
)
