from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "region")
w4 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        F.row_number().over(w).alias("row_seq"),
        F.max("sqft").over(w2).alias("city_max_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
        F.ntile(4).over(w5).alias("sqft_bucket"),
    )
)
