from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "is_active",
        "name",
        "age",
        F.concat_ws("-", "country", "name").alias("country_name_key"),
    )
    .orderBy("age", "id")
    .limit(3)
)
