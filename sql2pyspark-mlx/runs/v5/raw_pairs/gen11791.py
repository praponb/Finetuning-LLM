from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft").between(7355, 8261) & F.col("sqft").between(10022, 10577)) | F.col("store_name").like("Ol%"))
    .select(
        "opened_date",
        F.concat_ws("/", "city", "store_name").alias("city_store_name_key"),
    )
    .orderBy(F.col("city_store_name_key").desc())
)
