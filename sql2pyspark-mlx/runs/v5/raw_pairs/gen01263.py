from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2200)
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.last("revenue").over(w).alias("last_revenue"),
    )
)
