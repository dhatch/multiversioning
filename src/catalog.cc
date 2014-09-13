#include <catalog.h>

Catalog::Catalog() {

}

/*
 * If the given tableId already exists, preserve the old mapping and return 
 * false. Otherwise, this call succeeds.
 */
bool Catalog::PutTable(uint32_t tableId, MVTable *in) {
	if (tableMappings.find(tableId) == tableMappings.end()) {
		tableMappings[tableId] = in;
		return true;
	}
	return false;
}

/*
 * If the given tableId exists, return true, otherwise, return false.
 */
bool Catalog::GetTable(uint32_t tableId, MVTable **out) {
	auto iter = tableMappings.find(tableId);
	if (iter == tableMappings.end()) {
		*out = NULL;
		return false;
	}
	else {
		*out = iter->second;
		return true;
	}
}
