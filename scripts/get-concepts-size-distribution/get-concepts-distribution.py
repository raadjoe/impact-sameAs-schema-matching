from hdt import HDTDocument, IdentifierPosition
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

def getAllConcepts():
    set_all_concepts = set()
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "")
    for s,p,o in triples:
        set_all_concepts.add(o)
    return set_all_concepts


set_all_concepts = getAllConcepts()
print("# concepts", len(set_all_concepts))
counter = 0
for concept in set_all_concepts:
    (triples, cardinality) = hdt_lod.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", concept)
    print(counter, ";", concept, ";", cardinality, sep='')
    counter+=1
