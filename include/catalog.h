#ifndef 	CATALOG_H_
#define 	CATALOG_H_

#include <mv_table.h>
#include <unordered_map>

/*
 * The catalog is a mechanism to access the tables in the database. A single 
 * database contains a single instance of the catalog class.
 */
class Catalog
{

 private:
	
	// Store table mappings in this hash table.
	std::unordered_map<uint32_t, MVTable*> tableMappings;

 public:
	
	// Constructor.
	Catalog();

	// Add a table to the catalog.
	// 
	// param tableId: A 32-bit unique identifier to associate the table with.
	// param in: A reference to the table we're adding to the catalog.
	//
	// return value: true if the put succeeds, otherwise false.
	bool PutTable(uint32_t tableId, MVTable *in);

	// Get a reference to a table.
	//
	// param tableId: A 32-bit unique identifier to associate the table with.
	// param out: We return a reference to the table through this parameter.
	//
	// return value: true if we're able to find a table mapped to the given 
	//				 tableId.
	bool GetTable(uint32_t tableId, MVTable **out);	
};

#endif 		/* CATALOG_H_ */
