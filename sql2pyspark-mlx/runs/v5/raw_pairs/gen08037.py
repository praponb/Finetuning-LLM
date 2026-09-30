from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("row_seq"),
    )
)
