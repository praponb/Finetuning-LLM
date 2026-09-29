from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.rank().over(w2).alias("rnk"),
    )
)
