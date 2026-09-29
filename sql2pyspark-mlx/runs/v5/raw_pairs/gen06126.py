from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "is_active",
        "credit_limit",
        F.concat_ws("/", "city", "name").alias("city_name_key"),
    )
)
