#ifndef UTILITYFUNCTIONS_H
#define UTILITYFUNCTIONS_H

#include "min_mysql.h"

sqlResult getSqlProcessList(DB& db);
QString   queryEssay(const sqlRow& row, bool brief);
QString   queryEssay(const sqlResult& res, bool brief);

#endif // UTILITYFUNCTIONS_H
