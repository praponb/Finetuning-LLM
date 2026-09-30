from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
