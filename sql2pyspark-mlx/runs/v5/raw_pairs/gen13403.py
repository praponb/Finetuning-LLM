from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2024)
    .select(
        "id",
        "sqft",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
    )
)
