from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)
