from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull() & F.col("city").isin(["Tokyo", "Berlin"]))
    .withColumn("opened_date_weekofyear", F.weekofyear("opened_date"))
    .orderBy("region", "id")
    .limit(5)
)
