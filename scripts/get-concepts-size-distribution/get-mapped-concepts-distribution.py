#!/usr/bin/env python
# coding: utf-8

# In[1]:


from hdt import HDTDocument, IdentifierPosition
import pandas as pd
import numpy as np
import rocksdb
import codecs
import datetime
import pickle
import time
import operator
from collections import deque
def strict_handler(exception):
    return u"", exception.end
codecs.register_error("strict", strict_handler)


# In[2]:


PATH_LOD = "/scratch/wbeek/data/LOD-a-lot/data.hdt"
PATH_SAMEAS_NETWORK = "/home/jraad/ssd/data/identity-data/sameAsNetwork.hdt"
PATH_ID2TERMS_original = "/home/jraad/ssd/data/identity-data/id2terms_original.csv"
PATH_TERM2ID_original = "/home/jraad/ssd/data/identity-data/term2id_original.csv"
PATH_ID2TERMS_099 = "/home/jraad/ssd/data/identity-data-0_99/id2terms_0-99.csv"
PATH_TERM2ID_099 = "/home/jraad/ssd/data/identity-data-0_99/term2id_0-99.csv"
PATH_ID2TERMS_04 = "/home/jraad/ssd/data/identity-data-0_4/id2terms_0-4.csv"
PATH_TERM2ID_04 = "/home/jraad/ssd/data/identity-data-0_4/term2id_0-4.csv"


# In[3]:


# load the LOD-a-lot HDT file

hdt_lod = HDTDocument(PATH_LOD)


# In[4]:


# these identifiers will be used later to query the HDT file using their IDs

id_type = hdt_lod.convert_term("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", IdentifierPosition.Predicate)
id_sameAs = hdt_lod.convert_term("http://www.w3.org/2002/07/owl#sameAs", IdentifierPosition.Predicate)
id_subClassOf = hdt_lod.convert_term("http://www.w3.org/2000/01/rdf-schema#subClassOf", IdentifierPosition.Predicate)
id_equivalentClass = hdt_lod.convert_term("http://www.w3.org/2002/07/owl#equivalentClass", IdentifierPosition.Predicate)


# In[50]:


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


# In[6]:


DB_TERM_2_ID_original = rocksdb.DB("/home/jraad/ssd/data/DB_TERM_2_ID_original.db", rocksdb.Options(create_if_missing=True))
DB_TERM_2_ID_099 = rocksdb.DB("/home/jraad/ssd/data/DB_TERM_2_ID.db", rocksdb.Options(create_if_missing=True))
DB_TERM_2_ID_04 = rocksdb.DB("/home/jraad/ssd/data/DB_TERM_2_ID_0-4.db", rocksdb.Options(create_if_missing=True))


# In[44]:


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

        # TERMS in DB_TERM_2_ID_original start with "<" and end with ">"
        # TERMS in DB_TERM_2_ID_099 do not start with "<" and end with ">"
        # TERMS in DB_TERM_2_ID_04 do not start with "<" and end with ">

def getValueFromDB(key, DB): 
    if DB == DB_TERM_2_ID_original:
        key = "<" + key + ">"
    result = DB.get(serializeObject(key))
    if result != None:
        return "id_" + deSerializeObject(result)

    # function that takes a line from the term2ID file and return the term with its equivalence class ID
def splitTermAndID(line):
    parts = line.split(" ")
    if len(parts) < 2:
        return parts
    else:
        term = ""
        for i in range(len(parts) - 1):
            term = term + parts[i]
        return [term, parts[-1]]


# In[8]:


# returns the Jaccard measure of two sets
def jaccard(set1, set2):
    intersection = len(set(set1).intersection(set2))
    union = (len(set1) + len(set2)) - intersection
    if union ==0:
        # print("Both sets are empty")
        return None
    else:
        jacc = intersection / union
        # print("Leng:", len(set1), ", Length_Set2:", len(set2), "--> Inter:", intersection, "/ Union:", union,)
    return float("{0:.4f}".format(jacc))

# returns the Jaccard measure with description of two sets
def jaccard_details(set1, set2):
    intersection = len(set(set1).intersection(set2))
    union = (len(set1) + len(set2)) - intersection
    if union ==0:
        # print("Both sets are empty")
        return None
    else:
        jacc = float("{0:.4f}".format(intersection / union))
    return [intersection, union, jacc]

# given a URI inst as input, this function returns the set of Concepts in which inst is a member of
def getConceptsOfInstance(inst):
    set_classes = set()
    (triples, cardinality) = hdt_lod.search_triples(inst, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "")
    for s,p,o in triples:
        set_classes.add(o)
    return set_classes

# given a URI cl as input, this function returns the set of members of cl
def getInstancesOfConcept(cl):
    set_instances = set()
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", cl)
    for s,p,o in triples:
        set_instances.add(s)
    return set_instances
    
# given a URI cl as input, this function returns the ids of the set of members of cl
def getInstancesIDOfConcept(cl):
    id_cl = hdt_lod.convert_term(cl, IdentifierPosition.Object)
    set_instances_id = set()
    (triples, cardinality) = hdt_lod.search_triples_ids(0, id_type, id_cl)
    cnt = 0
    for s,p,o in triples:
        set_instances_id.add(s)
    return set_instances_id

def getAllSubClassesOfConcept(cl):
    queue = deque([cl])
    visited = {cl}
    while queue:
        v = queue.popleft()
        (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2000/01/rdf-schema#subClassOf", v)
        for s,p,o in triples:
            if s not in visited:
                queue.append(s)
                visited.add(s)
    return visited

def getInferredInstancesOfConcept(cl):
    subClasses = getAllSubClassesOfConcept(cl)
    set_inferred_instances = set()
    for subCl in subClasses:
        (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", subCl)
        for s,p,o in triples:
            set_inferred_instances.add(s)
    return set_inferred_instances


def getInferredInstancesIDOfConcept(cl):
    subClasses = getAllSubClassesOfConcept(cl)
    set_inferred_instances_id = set()
    for subCl in subClasses:
        id_subCl = hdt_lod.convert_term(subCl, IdentifierPosition.Object)
        if id_subCl != 0 :
            (triples, cardinality) = hdt_lod.search_triples_ids(0, id_type, id_subCl)
            if cardinality < 150000000:
                for s,p,o in triples:
                    set_inferred_instances_id.add(s)
            else:
                print("Program skipped concept '", subCl, "' with size:", cardinality)
    return set_inferred_instances_id

def getInferredInstancesIDOfConcept_details(cl):
    subClasses = getAllSubClassesOfConcept(cl)
    print(subClasses)
    set_inferred_instances_id = set()
    for subCl in subClasses:
        counter = 0
        id_subCl = hdt_lod.convert_term(subCl, IdentifierPosition.Object)
        if id_subCl != 0 :
            (triples, cardinality) = hdt_lod.search_triples_ids(0, id_type, id_subCl)
            for s,p,o in triples:
                counter+=1
                if counter % 10000000 == 0:
                    print(subCl,":", counter, "/", cardinality)
                set_inferred_instances_id.add(s)
    return set_inferred_instances_id
    
# unique indicates that 2 instances of a concept that are sameAs (after closure) will be considered as one instance
def getUniqueInstancesOfConcept(cl, database):
    set_instances_id = getInstances_ID_OfConcept(cl)
    unique_set_instances = set([])
    #string_instance = ""
    identity_set_id = ""
    for i in set_instances_id:
        uri = hdt_lod.convert_id(i, IdentifierPosition.Subject)
        #string_instance = string_instance+uri+";;"
        identity_set_id = getValueFromDB(uri, database)
        if identity_set_id == None:
            unique_set_instances.add(i)
        else:
            unique_set_instances.add(identity_set_id)
    #print(string_instance)
    #print(unique_set_instances)
    return unique_set_instances

# unique indicates that 2 instances of a concept that are sameAs (after closure) will be considered as one instance
def getUniqueInstancesOfInstanceSet(set_instances_id, database):
    unique_set_instances = set([])
    #string_instance = ""
    identity_set_id = ""
    for i in set_instances_id:
        uri = hdt_lod.convert_id(i, IdentifierPosition.Subject)
        #string_instance = string_instance+uri+";;"
        identity_set_id = getValueFromDB(uri, database)
        if identity_set_id == None:
            unique_set_instances.add(i)
        else:
            unique_set_instances.add(identity_set_id)
    #print(string_instance)
    #print(unique_set_instances)
    return unique_set_instances

def getLODMappings():
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2002/07/owl#equivalentClass", "")
    set_mappings = set([])
    counter = 0
    for s,p,o in triples:
        set_instances_1 = getInferredInstancesIDOfConcept(s)
        if len(set_instances_1) > 0 :
            set_instances_2 = getInferredInstancesIDOfConcept(o)
            if len(set_instances_2) > 0 :
                print(s + "(" + str(len(set_instances_1)) + ") <--> "+ o + "(" + str(len(set_instances_2)) + ")")
    return(set_mappings)

def getLODMappings2():
    start_time = time.time()
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2002/07/owl#equivalentClass", "")
    set_mappings = set([])
    counter = 0
    for s,p,o in triples:
        (triples, cardinality1) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", s)
        if cardinality1 > 0 :
            (triples, cardinality2) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", o)
            if cardinality2 > 0 :
                counter +=1
                print(s + "(" + str(cardinality1) + ") <--> "+ o + "(" + str(cardinality2) + ")")
    end_time = time.time()
    total_time = end_time - start_time
    print("Total time:", float("{0:.2f}".format(total_time)))
    return(counter)

def get_change(original_number, new_number):
    if original_number == new_number:
        return 0
    try:
        result = (abs(original_number - new_number) / original_number) * 100.0
        if new_number > original_number:
            return float("{0:.2f}".format(result))
        else:
            return float("{0:.2f}".format(-result))
    except ZeroDivisionError:
        return 9999

def checkSubSet(set1, set2):
    if set1.issubset(set2):
        return True
    else:
        if set2.issubset(set1):
            return True
        else:
            return False

def checkSubSet_LOD(c1, c2):
    (triples, cardinality) = hdt_lod.search_triples(c1, "http://www.w3.org/2000/01/rdf-schema#subClassOf", c2)
    if cardinality != 0:
        return True
    else:
        (triples, cardinality) = hdt_lod.search_triples(c2, "http://www.w3.org/2000/01/rdf-schema#subClassOf", c1)
        if cardinality != 0:
            return True
        else:
            return False


# In[66]:


# cl_1 = "http://aims.fao.org/aos/geopolitical.owl#self_governing"
# cl_2 = "http://vivoweb.org/ontology/core#Country"
# set_instances_1 = getInferredInstancesIDOfConcept(cl_1)
# unique_set_instances_1 = getUniqueInstancesOfInstanceSet(set_instances_1, DB_TERM_2_ID_original)
# set_instances_2 = getInferredInstancesIDOfConcept(cl_2)
# unique_set_instances_2 = getUniqueInstancesOfInstanceSet(set_instances_2, DB_TERM_2_ID_original)
# intersection = len(set(set_instances_1).intersection(set_instances_2))
# union = (len(set_instances_1) + len(set_instances_2)) - intersection
# print("Normal Jaccard (inter/union):", intersection, ",", union, "=", jaccard(set_instances_1, set_instances_2))
# print("-> Without sameAs sets are subsets?", set_instances_2.issubset(set_instances_1))
# intersection = len(set(unique_set_instances_1).intersection(unique_set_instances_2))
# union = (len(unique_set_instances_1) + len(unique_set_instances_2)) - intersection
# print("Enhanced Jaccard (inter/union):", intersection, ",", union, "=", jaccard(unique_set_instances_1, unique_set_instances_2))
# print("-> With sameAs sets are subsets?", unique_set_instances_2.issubset(unique_set_instances_1))


# In[ ]:


def getMappedConcepts():
    counter = 0
    returned_concepts = {}
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2002/07/owl#equivalentClass", "")
    for s,p,o in triples:
        if s != o:
            (triples, cardinality1) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", s)
            if cardinality1 != 0:
                (triples, cardinality2) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", o)
                if cardinality2 != 0:
                    counter+=1
                    #print(counter)
                    returned_concepts[s] = cardinality1
                    returned_concepts[o] = cardinality2
    #print("Finished:", counter)
    final_counter = 0
    for c in returned_concepts:
        final_counter+=1 
        print(final_counter, c, returned_concepts[c], "(no inf)")
        returned_concepts[c] = len(getInferredInstancesIDOfConcept(c))
        print(final_counter, c, returned_concepts[c], "(with inf)")
    return returned_concepts

mapped_concepts = getMappedConcepts()


# In[10]:


# sorted_mapped_concepts = sorted(mapped_concepts.items(), key=operator.itemgetter(1))
# sorted_mapped_concepts.reverse()
# del sorted_mapped_concepts[0]
# print("Length:", len(sorted_mapped_concepts))
# sorted_mapped_concepts


# In[16]:





# In[39]:


# # Jaccard on all elements

# def evaluate_LODMappings_Inference(sorted_mapped_concepts):
#     start_time = time.time()
#     print("row_id", "concept1", "concept2", "concept1_nbr_inst", "concept1_nbr_unique_instances", "concept2_nbr_inst", "concept2_nbr_unique_instances", "inter_no_sameas", "inter_with_sameas", "union_no_sameas", "union_with_sameas", "jacc_no_sameas", "jacc_with_sameas","isSubset_LOD", "isSubset_no_sameAs", "isSubset_with_sameas", "diff_with_sameas", sep=";~;")
#     copy_sorted_mapped_concepts = sorted_mapped_concepts.copy()
#     mappings_counter = 0
#     for concept1 in sorted_mapped_concepts:
#         concept1_uri = concept1[0]
#         del copy_sorted_mapped_concepts[0]
        
#         set_instances_1 = getInstancesIDOfConcept(concept1_uri)
#         len_set_instances_1 = len(set_instances_1)
#         unique_set_instances_1_all = getUniqueInstancesOfInstanceSet(set_instances_1, DB_TERM_2_ID_original)
        
#         for concept2 in copy_sorted_mapped_concepts:
#             concept2_uri = concept2[0]
#             (triples, cardinality) = hdt_lod.search_triples(concept1_uri, "http://www.w3.org/2002/07/owl#equivalentClass", concept2_uri)
#             if cardinality == 0:
#                 (triples, cardinality) = hdt_lod.search_triples(concept2_uri, "http://www.w3.org/2002/07/owl#equivalentClass", concept1_uri)
#             if cardinality != 0:
#                 is_subset_lod = checkSubSet_LOD(concept1_uri, concept2_uri)
#                 set_instances_2 = getInstancesIDOfConcept(concept2_uri)
#                 len_set_instances_2 = len(set_instances_2)
#                 mappings_counter+=1 
                   
#                 # no sameAs
#                 jacc_no_sameas = jaccard_details(set_instances_1, set_instances_2) 
#                 is_subset = checkSubSet(set_instances_1, set_instances_2) 
                
#                 # all sameAs links
#                 unique_set_instances_2 = getUniqueInstancesOfInstanceSet(set_instances_2, DB_TERM_2_ID_original)
#                 jacc_all_sameas = jaccard_details(unique_set_instances_1_all, unique_set_instances_2)
#                 diff_all_sameas = get_change(jacc_no_sameas[2], jacc_all_sameas[2])
#                 is_subset_all_sameAs = checkSubSet(unique_set_instances_1_all, unique_set_instances_2)
#                 unique_set_instances_2.clear()
                
#                 # output
#                 print(mappings_counter, concept1_uri, concept2_uri, len_set_instances_1, len_set_instances_2, jacc_no_sameas[0], jacc_no_sameas[1], jacc_no_sameas[2], jacc_all_sameas[0], jacc_all_sameas[1], jacc_all_sameas[2], jacc_099_sameas[0], jacc_099_sameas[1], jacc_099_sameas[2], jacc_04_sameas[0], jacc_04_sameas[1], jacc_04_sameas[2], is_subset_lod, is_subset, is_subset_with_sameAs, diff, sep=";~;")                      
                                                    
                
#     end_time = time.time()
#     total_time = end_time - start_time
#     print("---- Finished Evaluation in", float("{0:.2f}".format(total_time)) ,"seconds ----")
#     return copy_sorted_mapped_concepts  
                 


# In[40]:


# evaluate_LODMappings_Inference(sorted_mapped_concepts)


# In[9]:


# def replaceInstancesWithEqID():
#     all_instances = set()
#     (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "")
#     print("Number of rdf:type statements:", cardinality)
#     counter = 0
#     for s,p,o in triples:
#         counter +=1
#         if counter == 100:
#             break
#         print(s,p,o)


# In[11]:


# replaceInstancesWithEqID()


# In[ ]:




