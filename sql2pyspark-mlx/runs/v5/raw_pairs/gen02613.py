from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "New York") | ~F.col("city").isin(["New York", "Berlin"]))
    .select("id", "opened_date")
)
