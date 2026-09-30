from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "*",
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.lead("cost").over(w_part).alias("next_cost"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)
