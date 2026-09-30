from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-09-15", "2024-10-29"))
    .select(
        "id",
        "opened_date",
        "sqft",
        "region",
        "store_name",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_3"),
        F.last("store_name").over(w2).alias("last_store_name"),
    )
)
