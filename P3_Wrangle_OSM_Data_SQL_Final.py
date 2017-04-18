
# coding: utf-8

# # Extract Sample of OSM File

# In[1]:

#!/usr/bin/env python


import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "city_of_perth.osm"  # Replace this with your osm file
SAMPLE_FILE = "city_of_perth_sample.osm"

k = 12 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')


# # List of Files

# In[3]:

import os

def padStr( x, n ):
    '''Python print format with dot leader
    Reference:
    http://stackoverflow.com/questions/28588316/python-print-format-with-dot-leader'''  
    
    x += ' '
    return x + '.'*(n - len(x) )

folder = 'C:\Users\Dewi Octavia\Documents\P3_Wrangle OpenStreetMap Data\Final Project Files'
file_size = 0
for (path, dirs, files) in os.walk(folder):
    for file in files:
        filename = os.path.join(path,file)
        file_size = os.path.getsize(filename)
        print('%s %0.2f MB' %( padStr(file, 50), ((file_size)/(1024*1024.0))))


# # Iterative Parsing to Find Existing Tags

# In[1]:

# !/usr/bin/env python

"""
Your task is to use the iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data you can expect to have in the map.
Fill out the count_tags function. It should return a dictionary with the 
tag name as the key and number of times this tag can be encountered in 
the map as value.

Note that your code will be tested with a different data file than the 'example.osm'
"""
import xml.etree.cElementTree as ET
import pprint

def count_tags(filename):
    tags = {}
        
    for event, elem in ET.iterparse(filename):

        tag = elem.tag
       
        if tag not in tags.keys():
            tags[tag] = 1
        else:
            tags[tag] += 1
            
    return tags

def test():

    tags = count_tags('city_of_perth.osm')
    pprint.pprint(tags)
    

if __name__ == "__main__":
    test()


# # Auditing Street Types in Way Element

# In[5]:

import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

osm_file = open("city_of_perth.osm", 'r')

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE) #compile all last word
street_types = defaultdict(set)

expected = ["Street","Avenue","Boulevard","Drive","Court","Place", "Terrace"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    
    if m:
        street_type = m.group()
        
        if street_type not in expected:
            street_types[street_type].add(street_name)
            

def print_sorted_dict(d):
    keys = d.keys()
    keys = sorted(keys, key = lambda s: s.lower())
    
    for k in keys:
        v = d[k]
        print "%s: %d" % (k,v)
        

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def audit():
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                    
    pprint.pprint(dict(street_types))
                    
if __name__ == '__main__':
    audit()


# # Auditing Street Type in Node Element

# In[7]:

# import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

osm_file = open("city_of_perth.osm", 'r')

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE) #compile all last word
street_types = defaultdict(set)

expected = ["Street","Avenue","Boulevard","Drive","Court","Place", "Terrace"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    
    if m:
        street_type = m.group()
        
        if street_type not in expected:
            street_types[street_type].add(street_name)
            

def print_sorted_dict(d):
    keys = d.keys()
    keys = sorted(keys, key = lambda s: s.lower())
    
    for k in keys:
        v = d[k]
        print "%s: %d" % (k,v)
        

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def audit():
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                    
    pprint.pprint(dict(street_types))
                    
if __name__ == '__main__':
    audit()


# # Improving Street Names

# In[2]:

"""
Your task in this exercise has two steps:

- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to fix 
    the unexpected street types to the appropriate ones in the expected list.
    You have to add mappings only for the actual problems you find in this OSMFILE,
    not a generalized solution, since that may and will depend on the particular area you are auditing.
- write the update_name function, to actually fix the street name.
    The function takes a string with street name as an argument and should return the fixed name
    We have provided a simple test so that you see what exactly is expected
"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
import enchant
import string

OSMFILE = "city_of_perth.osm"
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\.\t\r\n]')
remove_numsym = re.compile(r'[a-zA-Z]+\s\w*')

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane",
            "Trail", "Parkway", "Commons", "Terrace"]

# UPDATE THIS VARIABLE
mapping = { "Cres": "Crescent",
           "St": "Street",
            "St.": "Street",
           "Ave" : "Avenue",
           "Rd." : "Road",
           "Ct" : "Court"
           }



def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

        return street_types
        
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def get_element(osm_file, tags=('node','way')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()
                             

def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in enumerate(get_element(osm_file)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                      
    osm_file.close()    
    return street_types


def hasNums(text):
    # hasNums function finds numbers in name
    return any(char.isdigit() for char in text)


def name_checklist(name):
    
    if name[-2:] == 'WA':
        name = name.replace('WA', "")
        
    if len(name.split()) == 1 and name[-3:] != 'way':
        name = name + ' Street' 
              
    if ',' in name:  # to remove ', suburb name' in street name
        fixed_name = name.split(',', 1)[0]
        name = fixed_name
    
    name = string.capwords(name)
    st_name = name.rsplit(None, 1)[-1]
    
    if st_name in mapping.keys(): # replace abbreviated street type to mapped street type
        st_name_aud = mapping[st_name]
        name = name.replace(st_name, st_name_aud)   
        
    elif st_name not in expected or st_name not in mapping.values():
        # suspected abbreviated street type could be misspelled instead
        d = enchant.Dict("en_AU")
        if d.check(st_name) == False: # if street type is misspelled
            suggest_list = d.suggest(st_name) # find suggested words
            replace_as = list(set(suggest_list).intersection(expected)) # find suggested word that matches expected list
            name = name.replace(st_name, replace_as[0]) 
    else:
        return name
    
    if hasNums(name) == True:
        # remove numbers and symbols from street name
        fixed_name = remove_numsym.search(name).group(0)
        name = fixed_name
    
    problem = problemchars.findall(name) # find any symbols such as comma or period
    if problem and (set(name.split()).intersection(['Road', 'Street'])):
        name = name.replace(problem[0],"")    
        
    return name


def and_name_checklist(name):
    name = string.capwords(name)
    
    if 'Streets' in name:
        fixed_name = name.replace('Streets', 'Street')
        name = fixed_name
        
    if 'Corner' in name:
        fixed_name = name.split('Corner ', 1)[1]
        name = fixed_name
        
        if 'Street' not in name:
            fixed_name = name + ' Street'
            name = fixed_name
    
    if name[-2:] == 'WA':
        name = name.replace('WA', "")
        
    if len(name.split()) == 1 and name[-3:] != 'way':
        name = name + ' Street' 
    
    if ',' in name:  # to remove ', suburb name' in street name
        fixed_name = name.split(',', 1)[0]
        name = fixed_name
    
    st_name = name.rsplit(None, 1)[-1]
    
    if st_name in mapping.keys():
        st_name_aud = mapping[st_name]
        name = name.replace(st_name, st_name_aud)
        
    else:
        d = enchant.Dict("en_AU")
        if d.check(st_name) == False: # if name is misspelled
            suggest_list = d.suggest(st_name) # find suggested words
            replace_as = list(set(suggest_list).intersection(expected)) # find suggested word that matches expected list
            name = name.replace(st_name, replace_as[0]) 
        else:
            name
    
    if hasNums(name) == True:
        fixed_name = remove_numsym.search(name).group(0)
        name = fixed_name
    
    problem = problemchars.findall(name) # find any symbols such as comma or period
    if problem and (set(name.split()).intersection(['Road', 'Street'])):
        name = name.replace(problem[0],"")    
        
    return name



def update_name(name, mapping):
    
    and_patterns = ['And', 'and', '&']
    
    if set(name.split()) & set(and_patterns):
        pattern = list(set(name.split()).intersection(and_patterns))[0]
        multiple_names = re.split(pattern, name)
            
        for i, item in enumerate(multiple_names):
            fixed_item = and_name_checklist(item)
            multiple_names[i] = fixed_item

        name = multiple_names
    else:
        name = name_checklist(name)
        
    return name
    
    
def test():
    st_types = audit(OSMFILE)

    for st_type, ways in st_types.iteritems():
        for name in ways:
            better_name = update_name(name, mapping)
            if name != better_name:
                print name, "=>", better_name
            if name == "West Lexington St.":
                assert better_name == "West Lexington Street"
            if name == "Baldwin Rd.":
                assert better_name == "Baldwin Road"


if __name__ == '__main__':

    test()


# # Preparing for SQL Database

# In[3]:

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus


OSM_PATH = "city_of_perth.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = {
    'node': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'lat': {'required': True, 'type': 'float', 'coerce': float},
            'lon': {'required': True, 'type': 'float', 'coerce': float},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'node_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    },
    'way': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'way_nodes': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'node_id': {'required': True, 'type': 'integer', 'coerce': int},
                'position': {'required': True, 'type': 'integer', 'coerce': int}
            }
        }
    },
    'way_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    }
}


# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

def is_num(s, field):
    # is_num function converts string into integer, float, or remain as string
    if field != 'version':
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return s
    else:
        return s


def find_key(value):
    m_colon = LOWER_COLON.search(value)
    m_prob = PROBLEMCHARS.search(value)
    
    if not m_prob:
        if m_colon:
            return value[value.index(':')+1:]
        else:
            return value
        
def find_type(type_value):
    m_colon = LOWER_COLON.search(type_value)
    m_prob = PROBLEMCHARS.search(type_value)
    
    if not m_prob:
        if m_colon:
            return type_value[: type_value.index(':')]
        else:
            return 'regular'
        
def update_postcode(value):
    if value.startswith('WA'):
        value = value.replace('WA ', "")
        return value
    else:
        return value
                    

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""
    
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = [] # Handle secondary tags the same way for both node and way elements
    count_waypos = 0
    
    # YOUR CODE HERE
    
    #===== For NODE  =======
    if element.tag == "node":
        for node in NODE_FIELDS:
            node_attribs[node] = is_num(element.attrib[node], node)
        
        single_tag = {}    
            
        for i in element.iter('tag'):  
                        
            # excludes some of tag keys to speed up code.
            if i.attrib['k'].startswith('addr:street'):
                k_value = i.attrib['k']
                v_value = update_name(i.attrib['v'], mapping)
                
                single_tag['id'] = is_num(element.attrib['id'], i)
                single_tag['key'] = find_key(k_value)
                single_tag['value'] = v_value
                single_tag['type'] = find_type(k_value)
                tags.append(single_tag.copy())
                
            elif i.attrib['k'] == 'addr:postcode':
                k_value = i.attrib['k']
                v_value = update_postcode(i.attrib['v'])
               
                single_tag['id'] = is_num(element.attrib['id'], i)
                single_tag['key'] = find_key(k_value)
                single_tag['value'] = v_value
                single_tag['type'] = find_type(k_value)
                tags.append(single_tag.copy())
                
            else:
                k_value = i.attrib['k']
                v_value = i.attrib['v']
                
                single_tag['id'] = is_num(element.attrib['id'], i)
                single_tag['key'] = find_key(k_value)
                single_tag['value'] = v_value
                single_tag['type'] = find_type(k_value)
                tags.append(single_tag.copy())
                
        print {'node': node_attribs, 'node_tags': tags}  
        
    #============ For WAY ========
    if element.tag == 'way':
        for field in WAY_FIELDS:
            way_attribs[field] = is_num(element.attrib[field],field)
        
        single_tag = {}    
            
        for i in element.iter('tag'):  
                
            if i.attrib['k'].startswith('addr:street'):    
                k_value = i.attrib['k']
                v_value = update_name(i.attrib['v'], mapping)

                single_tag['id'] = is_num(element.attrib['id'], i)
                single_tag['key'] = find_key(k_value)
                single_tag['value'] = v_value
                single_tag['type'] = find_type(k_value)
                tags.append(single_tag.copy())
            
            elif i.attrib['k'] == 'addr:postcode':
                k_value = i.attrib['k']
                v_value = update_postcode(i.attrib['v'])
                
                single_tag['id'] = is_num(element.attrib['id'], i)
                single_tag['key'] = find_key(k_value)
                single_tag['value'] = v_value
                single_tag['type'] = find_type(k_value)
                tags.append(single_tag.copy())
                
            else:
                k_value = i.attrib['k']
                v_value = i.attrib['v']

                single_tag['id'] = is_num(element.attrib['id'], i)
                single_tag['key'] = find_key(k_value)
                single_tag['value'] = v_value
                single_tag['type'] = find_type(k_value)
                tags.append(single_tag.copy())
        
        single_nd = {}
        for j in element.iter('nd'):
            single_nd['id'] = is_num(element.attrib['id'], j)
            single_nd['node_id'] = is_num(j.attrib['ref'], j)
            single_nd['position'] = count_waypos
                
            count_waypos += 1 
                
            way_nodes.append(single_nd.copy()) 
            
        print {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
            
            
    ### end of my code ###

    if element.tag == 'node':
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=False)


# # Import Tables into SQL DB

# In[4]:

### IMPORT NODES

import sqlite3
import csv
from pprint import pprint

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS nodes')
conn.commit()

# Create the table, specifying the column names and data types:

cur.execute('''
    CREATE TABLE nodes (
    id INTEGER PRIMARY KEY NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT)
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['lat'].decode("utf-8"),i['lon'].decode("utf-8"), i['user'].decode("utf-8"), 
              i['uid'].decode("utf-8"), i['version'].decode("utf-8"), i['changeset'].decode("utf-8"), 
              i['timestamp'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

# check that data imported correctly
#cur.execute('SELECT * FROM nodes')
#all_rows = cur.fetchall()
#print('1):')
#pprint(all_rows)

# close the connection
conn.close()


# In[5]:

### IMPORT NODES_TAGS

import sqlite3
import csv
from pprint import pprint

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS nodes_tags')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE nodes_tags (
    id INTEGER,
    key TEXT,
    value TEXT,
    type TEXT,
    FOREIGN KEY (id) REFERENCES nodes(id))
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('nodes_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO nodes_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

## check that data imported correctly
#cur.execute('SELECT * FROM nodes_tags')
#all_rows = cur.fetchall()
#print('1):')
#pprint(all_rows)

# close the connection
conn.close()


# In[6]:

### IMPORT WAYS

import sqlite3
import csv
from pprint import pprint

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS ways')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE ways (
    id INTEGER PRIMARY KEY NOT NULL,
    user TEXT,
    uid INTEGER,
    version TEXT,
    changeset INTEGER,
    timestamp TEXT)
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('ways.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['user'].decode("utf-8"),i['uid'].decode("utf-8"), i['version'].decode("utf-8"),
             i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

## check that data imported correctly
#cur.execute('SELECT * FROM ways')
#all_rows = cur.fetchall()
#print('1):')
#pprint(all_rows)

# close the connection
conn.close()


# In[7]:

### IMPORT WAYS_TAGS

import sqlite3
import csv
from pprint import pprint

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS ways_tags')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE ways_tags (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (id) REFERENCES ways(id)
)
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('ways_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO ways_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

## check that data imported correctly
#cur.execute('SELECT * FROM ways_tags')
#all_rows = cur.fetchall()
#print('1):')
#pprint(all_rows)

# close the connection
conn.close()


# In[8]:

### IMPORT WAYS_NODES

import sqlite3
import csv
from pprint import pprint

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS ways_nodes')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE ways_nodes (
    id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES ways(id),
    FOREIGN KEY (node_id) REFERENCES nodes(id))
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('ways_nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['node_id'].decode("utf-8"),i['position'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db)

# commit the changes
conn.commit()

# check that data imported correctly
#cur.execute('SELECT * FROM ways_nodes')
#all_rows = cur.fetchall()
#print('1):')
#pprint(all_rows)

# close the connection
conn.close()


# In[ ]:



