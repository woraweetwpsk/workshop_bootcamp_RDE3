import os
import configparser
import sqlalchemy
import pandas as pd
import requests

def read_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),'config.ini'))
    my_sql_host = config.get('MYSQL','MYSQL_HOST')
    my_sql_port = config.get('MYSQL','MYSQL_PORT')
    my_sql_user = config.get('MYSQL','MYSQL_USER')
    my_sql_password = config.get('MYSQL','MYSQL_PASSWORD')
    my_sql_db = config.get('MYSQL','MYSQL_DB')
    my_sql_charset = config.get('MYSQL','MYSQL_CHARSET')
    config_values = {
        'my_sql_host': my_sql_host,
        'my_sql_port': my_sql_port,
        'my_sql_user': my_sql_user,
        'my_sql_password': my_sql_password,
        'my_sql_db': my_sql_db,
        'my_sql_charset': my_sql_charset
    }
    return config_values

def import_url_tojson(url):
    r=requests.get(url)
    result=r.json()
    return result

if __name__ == "__main__":
    config_data = read_config()
    
    #create connect with sqlalchemy
    engine = sqlalchemy.create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user=config_data['my_sql_user'],
        password=config_data['my_sql_password'],
        host=config_data['my_sql_host'],
        port=config_data['my_sql_port'],
        db=config_data['my_sql_db'],
        )
    )
    
    with engine.connect() as connection:
        #import 3 Table => product,customer,transaction
        product = pd.read_sql("Select * From r2de3.product", engine).set_index("ProductNo")
        customer = pd.read_sql("Select * From r2de3.customer", engine)
        transaction = pd.read_sql("Select * From r2de3.transaction", engine)
        
    #merge 3 Table
    merged_transaction = transaction.merge(product,how="left",left_on="ProductNo",right_on="ProductNo").merge(customer,how="left",left_on="CustomerNo",right_on="CustomerNo")

    #import conversion rate 
    url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
    result_conversion_rate=import_url_tojson(url)
    #convert to DataFrame
    conversion_rate=pd.DataFrame(result_conversion_rate)
    #Drop Column 'id'
    conversion_rate=conversion_rate.drop(columns=['id'])
    #Convert date to dt.date ให้เหมือนกับ merged_transaction
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])
    
    #merge transaction and convertion_rate
    final_df = merged_transaction.merge(conversion_rate,how='left',left_on="Date",right_on='date')
        
    #add total_amount colomn
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    #add thb amount
    final_df['thb_amount'] = final_df['total_amount']*final_df['gbp_thb']
    #drop column ที่ไม่ได้ใช้
    final_df = final_df.drop(["Date","gbp_thb"],axis=1)
    #change name colume
    final_df.rename(columns={'TranscationNo':'transaction_no',
                             'ProductNo':'product_no',
                             'Price':'price',
                             'Quantity':'quantity',
                             'CustomerNo':'customer_no',
                             'ProductName':'product_name',
                             'Country':'country',
                             'Name':'name'},inplace=True)

   
    #sent to csv file and parquet      
    final_df.to_parquet("final.parquet",index=False)
    final_df.to_csv("final.csv",index=False)