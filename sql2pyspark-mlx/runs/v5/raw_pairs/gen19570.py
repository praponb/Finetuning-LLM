from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft") < 5246)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("row_seq"),
    )
)
