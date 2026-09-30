from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "New York", "Tokyo"]))
    .select(
        "id",
        "opened_date",
        "sqft",
        "store_name",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
