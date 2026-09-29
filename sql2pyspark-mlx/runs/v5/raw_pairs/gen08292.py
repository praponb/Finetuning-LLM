from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("sale_date", "id")

with_windows = (
    df_store_sales
    .withColumn("prev_units", F.lag("units").over(w_hist))
    .withColumn("next_units", F.lead("units").over(w_hist))
    .withColumn("next_revenue", F.lead("revenue").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "next_units",
        "next_revenue",
        "row_seq",
    )
)
