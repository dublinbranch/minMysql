#ifndef MIN_MYSQL_UTILITYFUNCTIONS_H
#define MIN_MYSQL_UTILITYFUNCTIONS_H

#include "min_mysql.h"
class DBDebugger : public DB {
      public:
	DBDebugger() = default;
	DBDebugger(const DBConf& conf);

	sqlResult getRunningQueries();
	sqlResult getProcessList();
};

sqlResult filterRunningQueries(const sqlResult& sqlProcessList);
QString   queryEssay(const sqlRow& row, bool brief);
QString   queryEssay(const sqlResult& res, bool brief);

#endif // UTILITYFUNCTIONS_H
