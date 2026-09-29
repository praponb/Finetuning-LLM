from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("cost") >= 90)
    .select(
        "id",
        "delivered_date",
        "cost",
        F.last("cost").over(w).alias("last_cost"),
        F.row_number().over(w2).alias("delivered_date_row"),
    )
    .orderBy(F.col("id").desc())
)
