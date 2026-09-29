from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city") != "New York")
    .select(
        "*",
        F.min("sqft").over(w).alias("last4_min_sqft"),
        F.sum("sqft").over(w).alias("last4_sum_sqft"),
        F.max("sqft").over(w).alias("last4_max_sqft"),
        F.count("sqft").over(w).alias("last4_count_sqft"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
