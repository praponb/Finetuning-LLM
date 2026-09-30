from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "is_active", "email", F.substring("name", 1, 4).alias("name_prefix"))
    .orderBy("name_prefix", "email", "id")
    .limit(5)
)
