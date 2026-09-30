from pyspark.sql import functions as F

result = (
    df_stores
    .select("sqft", F.date_trunc("week", "opened_date").alias("opened_date_period"))
)
