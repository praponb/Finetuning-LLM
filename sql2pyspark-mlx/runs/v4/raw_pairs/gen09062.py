from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region")

with_windows = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.avg("sqft").over(w2).alias("region_avg_sqft"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "cumulative_sqft",
        "region_avg_sqft",
        F.when(F.col("city").rlike("^(Berlin|Bangkok)$"), "GROUP_A").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "CORE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("cumulative_sqft")
)
