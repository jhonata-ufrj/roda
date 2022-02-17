import pymongo
from pymongo import collection

def connect_db():
    # Replace your URL here. Don't forget to replace the password.
    connection_url = 'mongodb+srv://user-jhon:eRPcQO6SAdh16CQr@database-vendas.dlvsx.mongodb.net/df_vendas?retryWrites=true&w=majority'

    cluster = pymongo.MongoClient(connection_url)
    
    # Database
    db = cluster["df_vendas"]
    # Table
    #collection = db["vendas"]
    return db
  
