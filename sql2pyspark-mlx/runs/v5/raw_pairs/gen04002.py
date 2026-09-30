from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-02-19")
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.lead("sqft", 1).over(w).alias("next_sqft"),
    )
    .orderBy("id", F.col("sqft").desc())
)
