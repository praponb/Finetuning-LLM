from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([1, 7, 9]))
    .select("id", "store_name", "opened_date", "sqft")
)
