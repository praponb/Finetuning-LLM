from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("sqft") > 5279)
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.last("sqft", ignorenulls=True).over(w_hist).alias("last_known_sqft"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)
