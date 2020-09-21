import pymongo
import json
from mongo_to_geojson import mongo_to_geojson 

#connection = pymongo.MongoClient("83.212.78.113", 27017)
#collection = connection.mongodb.geo
#collection1 = connection.mongodb.geotextualindex
#collection2 = connection.mongodb.geoindexbigdata
#collection3 = connection.mongodb.geotextualindexbigdata

connection = pymongo.MongoClient("localhost", 27017)
connection.admin.command('enableSharding', 'mongodb')
collection = connection.mongodb.geo
def import_data():
    file = open("restaurants-ver1.txt","r",encoding='utf-8')
    for line in file:
        fields = line.split("|")
        lat = float(fields[3])
        lon = float(fields[4])
        text = fields[5]
        newtext = text.split(',')
        netext1=[]
        for i in newtext:
            netext1.append(i.strip())
            
        details = {"type" : "Point", "coordinates": [lon,lat]}
        collection.insert_one({"Text":netext1,"location":details})
        #collection1.insert_one({"Text":netext1,"location":details})
        

def import_big_data():
    file = open("restaurants-ver1-augmentation.txt","r",encoding='utf-8')
    for line in file:
        fields = line.split("|")
        lat = float(fields[3])
        lon = float(fields[4])
        text = fields[5]
        newtext = text.split(',')
        netext1=[]
        for i in newtext:
            netext1.append(i.strip())
            
        details = {"type" : "Point", "coordinates": [lon,lat]}
        collection2.insert_one({"Text":netext1,"location":details})
        collection3.insert_one({"Text":netext1,"location":details})

# Main function that runs the program
def main():
    import_data()
    import_big_data()

if __name__ == "__main__":
    main()
