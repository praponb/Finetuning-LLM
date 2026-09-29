from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 10]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
    )
)

result = scored.select("id", "store_name", "city", "first_known_region")
