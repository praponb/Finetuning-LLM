from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.row_number().over(w).alias("opened_date_row"),
        F.lead("sqft", 2, 0).over(w).alias("next_sqft"),
    )
    .orderBy("city", F.col("sqft").desc())
)
