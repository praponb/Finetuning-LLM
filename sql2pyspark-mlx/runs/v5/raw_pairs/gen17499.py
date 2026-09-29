from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-07-03").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.when(F.col("sqft") >= 14038, "High").when(F.col("sqft") >= 7523, "Medium").otherwise("Low").alias("sqft_tier"),
        F.trim("region").alias("clean_region"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "city",
        "sqft",
        "resolved_region",
        F.avg("sqft").over(w).alias("sqft_avg_5_prior"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
        F.first("sqft", ignorenulls=True).over(w3).alias("first_known_sqft"),
    )
)
