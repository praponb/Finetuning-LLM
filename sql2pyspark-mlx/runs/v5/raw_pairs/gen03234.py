from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_stores
    .filter(F.col("sqft") <= 12784)
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w))
    .withColumn("sqft_avg_5", F.avg("sqft").over(w2))
)

result = (
    scored
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "first_known_sqft",
        "sqft_avg_5",
    )
    .orderBy(F.col("sqft").desc(), "sqft_avg_5")
)
