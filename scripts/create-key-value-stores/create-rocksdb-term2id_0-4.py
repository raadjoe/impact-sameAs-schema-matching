from hdt import HDTDocument, IdentifierPosition
import pandas as pd
import numpy as np
import rocksdb
import codecs
import datetime
import pickle
def strict_handler(exception):
    return u"", exception.end
codecs.register_error("strict", strict_handler)

PATH_LOD = "/scratch/wbeek/data/LOD-a-lot/data.hdt"
PATH_TERM2ID = "/home/jraad/ssd/data/identity-data-0_4/term2id_0-4.csv"

# load the LOD-a-lot HDT file
hdt_lod = HDTDocument(PATH_LOD)

# these identifiers will be used later to query the HDT file using their IDs
id_type = hdt_lod.convert_term("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", IdentifierPosition.Predicate)
id_sameAs = hdt_lod.convert_term("http://www.w3.org/2002/07/owl#sameAs", IdentifierPosition.Predicate)
id_subClassOf = hdt_lod.convert_term("http://www.w3.org/2000/01/rdf-schema#subClassOf", IdentifierPosition.Predicate)
id_equivalentClass = hdt_lod.convert_term("http://www.w3.org/2002/07/owl#equivalentClass", IdentifierPosition.Predicate)

# output some stats of LOD-a-lot
# we can query the HDT file using the term IDs (e.g. rdf:type and equivalentClass) or the URIs (e.g. subClassOf and sameAs)
print("# subjects:", "{:,}".format(hdt_lod.nb_subjects))
print("# predicates:", "{:,}".format(hdt_lod.nb_predicates))
print("# objects:", "{:,}".format(hdt_lod.nb_objects))
(triples, cardinality) = hdt_lod.search_triples("","","")
print("# triples:", "{:,}".format(cardinality))
(triples, cardinality) = hdt_lod.search_triples_ids(0, id_type, 0)
print("# rdf:type statements:", "{:,}".format(cardinality))
(triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "")
print("# rdfs:subClassOf statements:", "{:,}".format(cardinality))
(triples, cardinality) = hdt_lod.search_triples_ids(0, id_equivalentClass, 0)
print("# owl:equivalentClass statements:", "{:,}".format(cardinality))
(triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2002/07/owl#sameAs", "")
print("# owl:sameAs statements:", "{:,}".format(cardinality))

def serializeObject(obj):
    ser_obj = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    return ser_obj

def deSerializeObject(obj):
    deser_obj = pickle.loads(obj)
    return deser_obj

def insertToDB(key, value, DB):
    try:
        DB.put(serializeObject(key), serializeObject(value))
    except:
        print("Exception Occured in inserting to RocksDB", key)

def getValueFromDB(key, DB):
    result = DB.get(serializeObject(key))
    if result != None:
        return deSerializeObject(result)

def splitTermAndID(line):
    parts = line.split(" ")
    if len(parts) < 2:
        return parts
    else:
        term = ""
        for i in range(len(parts) - 1):
            term = term + parts[i]
        return [term, parts[-1]]

DB_TERM_2_ID_0_4 = rocksdb.DB("/home/jraad/ssd/data/DB_TERM_2_ID_0-4.db", rocksdb.Options(create_if_missing=True))

with open(PATH_TERM2ID) as f:
    line = f.readline()
    cnt = 0
    while line:
        line = f.readline()
        cnt += 1
        if cnt%1000000 == 0:
            print(cnt)
        splitted_line = splitTermAndID(line)
        insertToDB(splitted_line[0], splitted_line[1], DB_TERM_2_ID_0_4)
print("Finished creating database (TERM to ID 0.4). There is a total of ", "{:,}".format(cnt), "terms")
