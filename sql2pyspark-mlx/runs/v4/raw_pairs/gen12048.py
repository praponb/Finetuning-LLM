from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-12-29", "2025-09-01"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.lead("sqft", 2).over(w).alias("next_sqft"),
        F.percent_rank().over(w2).alias("sqft_pct_rank"),
    )
)
