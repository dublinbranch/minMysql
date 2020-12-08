#include "utilityfunctions.h"
#include "QtDebug"

sqlResult getSqlProcessList(DB& db) {
	auto sql = "show full processlist";
	auto res = db.query(sql);

	return res;
}

QString queryEssay(const sqlRow& row, bool brief) {
	QString time     = row["Time"];
	QString fullInfo = row["Info"];

	QString info;
	if (brief) {
		info = fullInfo.left(256);
	} else {
		info = fullInfo;
	}

	//looks as a reasonable lenght to avoid cluttering and mostly understand was is going on
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
