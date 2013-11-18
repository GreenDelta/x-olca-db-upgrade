x-olca-db-upgrade
=================
This is a package for converting an openLCA 1.3 database to the database format
of openLCA 1.4.

Done
----
The following entities are mapped:

* Actors
* Categories
* Exchanges
* Flows + flow property factors
* Flow properties
* Locations
* Processes + documentation
* Sources
* Units
* Unit groups
* product systems, process links, processes
* LCIA methods, categories, and factors
* parameters (*)


TODO
----
* process sources
* projects
* technology comments
* allocation factors
* cost categories and process cost entries
* NW sets and factors
* process groups

(*) Product system parameters cannot be converted to the new openLCA database 
format. In openLCA 1.3 product system parameters were just matched by name 
where in openLCA 1.4 product system parameters are redefinitions of existing
global or process parameters.