from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.month("sale_date").isin([2, 8, 10]))
        .select("store_id")
    )
)
