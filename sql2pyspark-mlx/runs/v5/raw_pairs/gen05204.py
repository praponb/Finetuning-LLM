from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.last("region", ignorenulls=True).over(w3).alias("last_known_region"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "opened_date",
        "running_sqft",
        F.when(F.col("store_name").rlike("^(Mall Central|Old Town)$"), "OTHER_ZONE").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "PRIORITY").otherwise("GROUP_B").alias("store_name_zone"),
    )
)
