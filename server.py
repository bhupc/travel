import httplib
import pprint
import xml.dom.minidom
from xml.dom.minidom import Node
import pickle
#import MySQLdb
import time
import sys


gCurrentWaitTime = 1;
MAX_OBJS_IN_FILE = 500;

def escapeThrottle():
  global gCurrentWaitTime;
  gCurrentWaitTime *= 2;
  print "Current Wait time is ", gCurrentWaitTime;
  time.sleep(gCurrentWaitTime);

def unThrottle():
  global gCurrentWaitTime;
  if gCurrentWaitTime >= 2:
    gCurrentWaitTime /= 2;


def sendHTTPRequest(method, host, path):
  conn = httplib.HTTPConnection(host);
  conn.request(method, path);
  resp = conn.getresponse();
  if resp.status == 200:
    unThrottle();
    return resp;
  else:
    escapeThrottle();
  return sendHTTPRequest(method, host, path); 

def findPlaceByName(name):
  path = "/api/places?name="+name;
  resp = sendHTTPRequest("GET", "apigateway.lonelyplanet.com", path);
  return resp.read();


def findPoiByID(poiID):
  path = "/api/pois/"+poiID;
  resp = sendHTTPRequest("GET", "apigateway.lonelyplanet.com", path);
  return resp.read();

def parseXML(xmlData):
  doc = xml.dom.minidom.parseString(xmlData);
  place_childTags = ["id", "full-name", "short-name", "north-latitude", "short-latitude", "east-longitude", "west-longitude"];
  
  places_obj = [];
  place_count = 0;
  places_list = doc.getElementsByTagName("places");
  places = places_list[0];
  for place in places.getElementsByTagName("place"):
    place_obj = {};
    for tag_node in place.childNodes:
      if tag_node.localName != None:
        if tag_node.childNodes.len() > 0:
          tag_text_node = tag_node.childNodes[0];
          place_obj[tag_node.localName] = tag_text_node.data;
    places_obj.append(place_obj);
  return places_obj;


def parseXMLForPOI(xmlData):
  doc = xml.dom.minidom.parseString(xmlData);
  poi = doc.getElementsByTagName("poi")[0];
  poi_obj = {};
  
  poi_obj["review"] = [];
  for tag_node in poi.childNodes:
    #print "tag = %s\n" %(tag_node.localName);
    if tag_node.localName != None and tag_node.childNodes.length > 0:
      if tag_node.localName == "poi-type":
        poi_obj["poi-type"] = tag_node.childNodes[0].data;

      if tag_node.localName == "name":
        poi_obj["name"] = tag_node.childNodes[0].data;

      if tag_node.localName == "digital-longitude":
        poi_obj["digital-longitude"] = tag_node.childNodes[0].data;
      
      if tag_node.localName == "digital-latitude":
        poi_obj["digital-latitude"] = tag_node.childNodes[0].data;
      

      if tag_node.localName == "address":
        poi_obj["address"] = {};
        poi_obj_address = poi_obj["address"];
        for address_child in tag_node.childNodes:
          if address_child.childNodes.length > 0:
            poi_obj_address[address_child.localName] = address_child.childNodes[0].data;
      
      if tag_node.localName == "review":
        review_child_node = tag_node.childNodes[0];
        if review_child_node.childNodes.length > 0:
          poi_obj["review"].append(review_child_node.childNodes[0].data);
      
      if tag_node.localName == "telephones":
        poi_obj["telephones"] = [];
        for telephone_node in tag_node.childNodes:
          telephone_obj = {};
          for telephone_child_node in telephone_node.childNodes:
            if telephone_child_node.childNodes.length > 0:
              telephone_obj[telephone_child_node.localName] = telephone_child_node.childNodes[0].data;
          poi_obj["telephones"].append(telephone_obj);
        
  return poi_obj;

def printPlacesObject(places):
  i=0;
  for  place in places:
    print "Place\n"
    for t in place:
      print "%s = %s\n" %(t, place[t]);


def printPOIObject(poi_obj):
  attr_list = ["name", "poi-type", "digital-longitude", "digital-latitude", "address", "review", "telephones"];
  for attr in attr_list:
    if attr == "address" or attr == "review" or attr == "telephones":
      print poi_obj[attr]
    else:
      print "\n%s not found\n" %(attr);


# dumping obj to a file

def dumpToFile(obj, fp):
    pickle.dump(obj, fp);	




def runDBQuery(query):
  #pickle.dump(input_obj, fp);  
  print "Trying to connect...";
  db = MySQLdb.connect(host="localhost", port=3306, user="root", passwd="root", db="iTravel");
  if db == None:
    print "Could not connect to the database";
    return;
  print "Connected..";
  cursor = db.cursor();
  cursor.execute(query);
  data = cursor.fetchone();
  db.close();

def loadObjectFromDB(fp):
  return pickle.load(fp);


def getPlaceExample():
  xmlData = findPlaceByName("sanfrancisco");
  #print xmlData;
  if xmlData != None:
    places = parseXML(xmlData);
    printPlacesObject(places);
    fp = open("db", "wb");
    persistObjectToDB(places, fp);

def getPOIExample(poiID):
  xmlData = findPoiByID(poiID);
  #print xmlData;
  poi = {};
  if xmlData != None:
    poi = parseXMLForPOI(xmlData);
    poi["poi-id"] = poiID;
    return poi;
    #printObject(places);
    #fp = open("db", "wb");
    #persistObjectToDB(places, fp);


def getPOIList(place,category):
  path = "/api/places/"+place+"/pois?poi_type="+category;
  print path;
  resp = sendHTTPRequest("GET","apigateway.lonelyplanet.com",path);
  return resp.read();

def parseXMLForPOIList(xmlData):

 doc = xml.dom.minidom.parseString(xmlData);
 poilist_childTags = ["id","poi-type","name","digital-latitude","digital-longitude"]; 
 pois_obj =[];
 poi_list= doc.getElementsByTagName("pois");
 pois = poi_list[0];
 for  poi in pois.getElementsByTagName("poi"):
    poi_obj = {};
    for tag_node in poi.childNodes:
      if tag_node.localName != None:
        if tag_node.childNodes.length > 0:
          tag_text_node = tag_node.childNodes[0];
          poi_obj[tag_node.localName] = tag_text_node.data;
    pois_obj.append(poi_obj);
 return pois_obj;


def encodeString(name):
  print "encoding..."
  try: 
    name.encode('iso-8859-15') 
  except UnicodeEncodeError: 
    name.encode('iso-8859-1') 
  return name;

def persistPOIObjectToDB(poi):
  poi["poi-id"] = encodeString(poi["poi-id"]);
  poi["name"] = encodeString(poi["name"]);
  poi["poi-type"] = encodeString(poi["poi-type"]);
  print poi["poi-id"];
  print poi["name"];
  print poi["poi-type"] ;
  query = "insert into POINT_OF_INTERESTS (POI_ID, POI_NAME, POI_TYPE) values (%s, \"%s\", \"%s\") " %(poi["poi-id"], poi["name"], poi["poi-type"]);
  print query;
  runDBQuery(query);
  attr_list = ["poi-name", "poi-type", "digital-longitude", "digital-latitude", "address", "review", "telephones"];


def POIForPlaceAndCategory(placeID, category):
  global MAX_OBJS_IN_FILE;

  xmlData  = getPOIList(placeID , category);
  currentFileIndex = 0;
  filePrefix = "%s_%s" %(placeID, category);
  totalAdded = 0;
  if xmlData != None:
    pois = parseXMLForPOIList(xmlData);
    ctr = 0;
    this_round = [];
    for poi in pois:
      id1 = poi["id"];
      print "id1 = ", id1;
      poi_forId = getPOIExample(id1);
      this_round.append(poi_forId);
      totalAdded += 1;
      print "Total Records added = ", totalAdded;
      ctr = ctr + 1;
      if ctr == MAX_OBJS_IN_FILE:
        fileName = "%s_%d" %(filePrefix, currentFileIndex);
        fp = open(fileName, "wb");
        dumpToFile(this_round, fp);
        this_round = [];
        currentFileIndex += 1;
        ctr = 0;
    fileName = "%s_%d" %(filePrefix, currentFileIndex);
    fp = open(fileName, "wb");
    dumpToFile(this_round, fp);


def loadFromFile(fp):
  return pickle.load(fp);
  

def main():
  if len(sys.argv) != 3:
    print "Error. Enter placeID  and category as id";
    return;

  POIForPlaceAndCategory(sys.argv[1], sys.argv[2]);
  #fp = open("361858_Eat_0", "r");
  #poi_array = loadFromFile(fp);
  
main();

