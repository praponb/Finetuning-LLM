from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w_ordered = Window.partitionBy("store_id").orderBy("units", "id")

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 4]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

result = (
    filtered
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)
