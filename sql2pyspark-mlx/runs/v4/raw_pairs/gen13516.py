from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(13375, 15667))
    .select("opened_date")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Bangkok", "Berlin", "London"]))
        .select("opened_date")
    )
    .distinct()
    .orderBy("opened_date")
)
