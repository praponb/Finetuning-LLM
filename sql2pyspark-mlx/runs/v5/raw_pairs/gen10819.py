from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3700)
    .select(
        "id",
        "store_id",
        "revenue",
        "units",
        F.last("units").over(w).alias("last_units"),
    )
)
