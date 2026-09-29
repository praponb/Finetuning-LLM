from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("units") < 39)
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("st.stddev_units"))
    .orderBy("opened_date")
)
