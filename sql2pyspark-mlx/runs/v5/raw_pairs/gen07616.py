from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2, 3, 6]))
        .select("sale_date")
    )
)
