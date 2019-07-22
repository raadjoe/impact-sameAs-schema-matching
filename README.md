# On the impact of sameAs on schema matching
This repository contains all the Python scripts and data necessary to replicate our experiments of our paper "On the impact of sameAs on schema matching" authored by [Joe Raad](http://joe-raad.com), [Erman Acar](https://research.vu.nl/en/persons/erman-acar), and [Stefan Schlobach](http://www.few.vu.nl/~schlobac/).

### With these experiments we aim at answering the two following research questions:

Q1. Does the inclusion of instance-level interlinks enhance instance-based schema alignments? (w and w/o considering the transitive closure of the class subsumption relation.)

Q2. Is there a correlation between the quality of the instance-level interlinks and the quality of the resulting schema alignments?

### A number of external resources are necessary for replicating these experiments:

> 1. Download the [LOD-a-lot dataset](http://lod-a-lot.lod.labs.vu.nl).

This data set contains 28.3 billion triples collected from the 2015 LOD Laundromat crawl of over 650K data documents from the Web. It is exposed in an HDT file that is 524GB in size (including its additional index), and is publicly accessible via an [LDF interface](http://krr.triply.cc/krr/lod-a-lot). 

> 2. Download the [Equivalence Classes](https://zenodo.org/record/3227976).

This data set of equivalence classes results from the closure of all 558 million owl:sameAs links in the [sameAs.cc](http://sameas.cc) data set. This data set also contains two additional set of equivalence classes resulted (a) after discarding all owl:sameAs links with an error degree >0.99, and (b) after discarding all owl:sameAs links with an error degree >0.4.

> 3. Install the [HDT Python library](https://pypi.org/project/hdt/)

This library allows to read and query HDT document with ease in Python
