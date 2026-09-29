from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("store_name").like("%Flagshi%"))
    .select(
        "id",
        "sqft",
        "store_name",
        "region",
        "opened_date",
        F.last("store_name").over(w).alias("last_store_name"),
        F.rank().over(w2).alias("rnk"),
    )
)
