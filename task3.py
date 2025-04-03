from pyspark.sql import SparkSession

if __name__=="__main__":
    spark=SparkSession.builder.master("local").appName("assignment").getOrCreate()
    sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    sales_data = [
        ('A', '2021-01-01', '1'),
        ('A', '2021-01-01', '2'),
        ('A', '2021-01-07', '2'),
        ('A', '2021-01-10', '3'),
        ('A', '2021-01-11', '3'),
        ('A', '2021-01-11', '3'),
        ('B', '2021-01-01', '2'),
        ('B', '2021-01-02', '2'),
        ('B', '2021-01-04', '1'),
        ('B', '2021-01-11', '1'),
        ('B', '2021-01-16', '3'),
        ('B', '2021-02-01', '3'),
        ('C', '2021-01-01', '3'),
        ('C', '2021-01-01', '1'),
        ('C', '2021-01-07', '3')]
    sales_col=["customer_id", "order_date", "product_id"]
    sales_df=spark.createDataFrame(sales_data,schema=sales_col)
    sales_df.show()
    menu_data = [('1', 'palak_paneer', 100),
                 ('2', 'chicken_tikka', 150),
                 ('3', 'jeera_rice', 120),
                 ('4', 'kheer', 110),
                 ('5', 'vada_pav', 80),
                 ('6', 'paneer_tikka', 180)]
    # cols
    menu_cols = ['product_id', 'product_name', 'price']
    menu_df=spark.createDataFrame(menu_data,schema=menu_cols)
    menu_df.show()
    members_data = [('A', '2021-01-07'),
                    ('B', '2021-01-09')]
    # cols
    members_cols = ["customer_id", "join_date"]
    members_df=spark.createDataFrame(members_data,schema=members_cols)
    members_df.show()

    """
    NOW HERE ARE THE TASKS : 

1) What is the total amount each customer spent at the restaurant?

2) How many days has each customer visited the restaurant?

3) What was each customer’s first item from the menu?

4) Find out the most purchased item from the menu and how many times the customers purchased it.

5) Which item was the most popular for each customer?

6) Which item was ordered first by the customer after becoming a restaurant member?

7) Which item was purchased before the customer became a member?

8) What is the total items and amount spent for each member before they became a member?

9) If each rupee spent equates to 10 points and item ‘jeera_rice’ has a 2x points multiplier, find out how many points each customer would have.


10) Create the complete table with all data and columns like customer_id, order_date, product_name, price, and member(Y/N).


11) We also require further information about the ranking of customer products. The owner does not need the ranking for non-member purchases, so he expects null ranking values for the records when customers still need to be part of the membership program.
    """