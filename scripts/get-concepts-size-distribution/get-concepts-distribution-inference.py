from hdt import HDTDocument, IdentifierPosition
from collections import deque
import pandas as pd
import numpy as np
import rocksdb
import codecs
import datetime
def strict_handler(exception):
    return u"", exception.end
codecs.register_error("strict", strict_handler)

PATH_LOD = "/scratch/wbeek/data/LOD-a-lot/data.hdt"
PATH_SAMEAS_NETWORK = "/home/jraad/ssd/data/identity-data/"
PATH_ID2TERMS_099 = "/home/jraad/ssd/data/identity-data-0_99/id2terms_0-99.csv"
PATH_TERM2ID_099 = "/home/jraad/ssd/data/identity-data-0_99/term2id_0-99.csv"

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


def getAllDirectlyInstantiatedConcepts():
    print("Getting all directly instantiated concepts...")
    set_all_concepts = set()
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "")
    inst_cnt = 0
    for s,p,o in triples:
        inst_cnt += 1
        if inst_cnt % 10000000 == 0:
            print(inst_cnt, "/", cardinality)
        set_all_concepts.add(o)
    print("... Done")
    return set_all_concepts

def getAllConcepts():
    set_all_concepts = getAllDirectlyInstantiatedConcepts()
    print("Getting all concepts extracted from subClassOf relations...")
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "")
    cncpt_cnt = 0
    for s,p,o in triples:
        cncpt_cnt += 1
        if cncpt_cnt % 100000 == 0:
            print(cncpt_cnt, "/", cardinality)
        set_all_concepts.add(s)
        set_all_concepts.add(o)
    print("... Done")
    return set_all_concepts

set_all_concepts = getAllConcepts()
print("---------")
print("# all concepts", len(set_all_concepts))
print("---------")

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

def getInferredInstancesIDOfConcept(cl):
    subClasses = getAllSubClassesOfConcept(cl)
    set_inferred_instances_id = set()
    for subCl in subClasses:
        id_subCl = hdt_lod.convert_term(subCl, IdentifierPosition.Object)
        if id_subCl != 0 :
            (triples, cardinality) = hdt_lod.search_triples_ids(0, id_type, id_subCl)
            for s,p,o in triples:
                set_inferred_instances_id.add(s)
    return set_inferred_instances_id


print("Getting all inferred instances from each concept...")
counter = 0
for concept in set_all_concepts:
    set_instances = getInferredInstancesIDOfConcept(concept)
    print(counter, concept, len(set_instances), sep=';;;')
    counter+=1
print("Finished printing all concepts size distribution with rdfs:subClassOf inference")
