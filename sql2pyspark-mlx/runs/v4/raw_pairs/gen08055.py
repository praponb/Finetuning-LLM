from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "store_id")
    .intersectAll(
        df_store_sales
        .filter(F.month("sale_date").isin([1, 3, 9]))
        .select("id", "store_id")
    )
)
