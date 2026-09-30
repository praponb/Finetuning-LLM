from pyspark.sql import functions as F

max_opened_date = df_stores.filter(F.col("store_name").isNotNull()).agg(F.max("opened_date")).first()[0]

result = (
    df_stores
    .filter(F.col("opened_date") == max_opened_date)
    .select("id", "opened_date", "store_name")
    .orderBy(F.col("store_name").desc(), "id")
    .limit(5)
)
