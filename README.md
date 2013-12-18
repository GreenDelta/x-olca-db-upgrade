x-olca-db-upgrade
=================
This is a package for converting an openLCA 1.3 database to the database format
of openLCA 1.4.

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
* Product systems, process links, processes
* LCIA methods, categories, and factors
* Parameters (*)
* NW sets and factors
* Projects
* Allocation factors (**)
* Cost categories and process cost entries

(*) Product system parameters cannot be converted to the new openLCA database 
format. In openLCA 1.3 product system parameters were just matched by name 
where in openLCA 1.4 product system parameters are redefinitions of existing
global or process parameters.

(**) In openLCA 1.4 it is possible to create physical, economic, and causal
allocation factors for a process. In openLCA 1.3 only the factors of one 
allocation method could be used for a process. The conversion creates factors
for all allocation methods for a process. Thus, the factors of converted 
multi-output processes should checked by the user after the conversion.

Open issues
-----------
Process groups (from the analysis editor) are currently not mapped.

Usage
-----
You can migrate an openLCA 1.3.x MySQL database to an openLCA 1.4 MySQL
database:

     String oldDbUrl = "jdbc:mysql://localhost:3306/old_database";
     IDatabase oldDb = new MySQLDatabase(oldDbUrl, "root", "");
     String newDbUrl = "jdbc:mysql://localhost:3306/new_database";
     IDatabase newDb = new MySQLDatabase(newDbUrl, "root", "");
     Update update = new Update(oldDb, newDb);
     update.run();
     oldDb.close();
     newDb.close();

Additionally, you can export an openLCA 1.3.x MySQL database to an openLCA 1.4
Derby database which is packed during the export into an `zolca` file:

     String oldDbUrl = "jdbc:mysql://localhost:3306/old_database";
     IDatabase oldDb = new MySQLDatabase(oldDbUrl, "root", "");
     File exportFile = new File("C:/.../new_database.zolca");
     FileExport fileExport = new FileExport(oldDb, exportFile);
     fileExport.run();
     oldDb.close();