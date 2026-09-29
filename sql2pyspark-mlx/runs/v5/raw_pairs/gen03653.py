from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("South"))))
    .select(
        "*",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "city",
        "sqft",
        "days_since_opened_date",
        "region_clean",
        F.sum("sqft").over(w_ordered).alias("running_sum_sqft"),
        F.count("sqft").over(w_ordered).alias("running_count_sqft"),
        F.min("sqft").over(w_ordered).alias("running_min_sqft"),
        F.sum("sqft").over(w_ordered_2).alias("cumulative_sqft"),
    )
)
