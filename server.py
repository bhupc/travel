import httplib
import pprint
import xml.dom.minidom
from xml.dom.minidom import Node
import pickle
import MySQLdb


def sendHTTPRequest(method, host, path):
  conn = httplib.HTTPConnection(host)
  conn.request(method, path)
  r1 = conn.getresponse()
  return r1;


def findPlaceByName(name):
  path = "/api/places?name="+name;
  resp = sendHTTPRequest("GET", "apigateway.lonelyplanet.com", path);
  if resp.status == 200:
    return resp.read();
  print "Got non 200 response status";
  return None;


def findPoiByID(poiID):
  path = "/api/pois/"+poiID;
  resp = sendHTTPRequest("GET", "apigateway.lonelyplanet.com", path);
  if resp.status == 200:
    return resp.read();
  print "Got non 200 response status";
  return None;

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
    print "tag = %s\n" %(tag_node.localName);
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
  attr_list = ["poi-name", "poi-type", "digital-longitude", "digital-latitude", "address", "review", "telephones"];
  for attr in attr_list:
    if attr == "address" or attr == "review" or attr == "telephones":
      print poi_obj[attr]
    else:
      print "\n%s not found\n" %(attr);





def runDBQuery(query):
  #pickle.dump(input_obj, fp);  
  print "Trying to connect...";
  db = MySQLdb.connect(host="10.32.115.211", port=3306, user="root", passwd="root", db="iTravel");
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
  print xmlData;
  if xmlData != None:
    places = parseXML(xmlData);
    printPlacesObject(places);
    fp = open("db", "wb");
    persistObjectToDB(places, fp);

def getPOIExample(poiID):
  xmlData = findPoiByID(poiID);
  print xmlData;
  if xmlData != None:
    poi = parseXMLForPOI(xmlData);
    poi["poi-id"] = poiID;
  return poi;
    #printObject(places);
    #fp = open("db", "wb");
    #persistObjectToDB(places, fp);



def persistPOIObjectToDB(poi):
   
  print poi["poi-id"];
  print poi["name"];
  print poi["poi-type"] ;
  query = "insert into POINT_OF_INTERESTS (POI_ID, POI_NAME, POI_TYPE) values (%s, %s, %s) " %(poi["poi-id"], poi["name"], poi["poi-type"]);
  print query;
  runDBQuery(query);
  attr_list = ["poi-name", "poi-type", "digital-longitude", "digital-latitude", "address", "review", "telephones"];


poi = getPOIExample("384361");
persistPOIObjectToDB(poi);


