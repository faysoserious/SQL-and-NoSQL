# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 18:27:40 2017

@author: fxie_
"""

from pymongo import MongoClient
client = MongoClient('localhost',27017)
db = client.Northwind
collection = db.customer
result = collection.find_one()
print(type(result))
print(result)
print(client.database_names())
