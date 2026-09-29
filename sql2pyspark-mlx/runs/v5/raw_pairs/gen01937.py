from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2025-05-01") & F.col("sqft").between(9250, 9618))
    .withColumn("clean_region", F.trim("region"))
    .withColumn("city_upper", F.upper("city"))
    .orderBy("store_name", "city_upper")
)
