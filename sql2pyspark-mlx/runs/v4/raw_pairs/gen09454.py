from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("sqft") <= 4566) & (F.col("sqft") > 9482)))
    .select("store_name", F.initcap(F.trim("store_name")).alias("store_name_title"))
)
