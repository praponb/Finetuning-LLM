from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(((F.col("sqft") <= 12876) & (F.col("region") == "East")) | F.col("opened_date").between("2023-04-01", "2025-03-01")))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
    .withColumn("region_city", F.concat("region", F.lit(" - "), "city"))
)
