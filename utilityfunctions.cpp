#include "utilityfunctions.h"
#include "QtDebug"

sqlResult filterRunningQueries(const sqlResult& sqlProcessList) {
	sqlResult res;
	for (auto& sql : sqlProcessList) {
		auto command = sql.get("Command");
		if ((command == QBL("Query")) or (command == QBL("Connect"))) {
			res.push_back(sql);
		}
	}
	return res;
}

QString queryEssay(const sqlRow& row, bool brief) {
	QString time     = row["Time"];
	QString fullInfo = row["Info"];

	QString info;
	if (brief) {
		//looks as a reasonable lenght to avoid cluttering and mostly understand was is going on
		info = fullInfo.left(256);
	} else {
		info = fullInfo;
	}

	return QSL("for %1 s : %2 ")
	           .arg(time)
	           .arg(info) +
	       "\n";
}

QString queryEssay(const sqlResult& res, bool brief) {
	QString msg;
	for (auto&& query : res) {
		msg.append(queryEssay(query, brief));
	}
	return msg;
}

DBDebugger::DBDebugger(const DBConf& conf) {
	setConf(conf);
}

sqlResult DBDebugger::getRunningQueries() {
	return filterRunningQueries(getProcessList());
}

sqlResult DBDebugger::getProcessList() {
	auto sql = "show full processlist";
	auto res = query(sql);
	return res;
}
