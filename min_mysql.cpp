#include "min_mysql.h"
#include "QStacker/qstacker.h"
#include "mysql/mysql.h"
#include <QDataStream>
#include <QDateTime>
#include <QDebug>
#include <QElapsedTimer>
#include <QFile>
#include <QMap>
#include <QRegularExpression>
#include <QScopeGuard>
#include <fileFunction/filefunction.h>
#include <fileFunction/serialize.h>
#include <memory>
#include <mutex>
#include <poll.h>
#include <unistd.h>

DB::SharedState DB::sharedState;
using namespace std;
//I (Roy) really do not like reading warning, so we will now properly close all opened connection!
class ConnPooler {
      public:
	void                        addConnPool(st_mysql* conn);
	void                        removeConn(st_mysql* conn);
	void                        closeAll();
	const map<st_mysql*, bool>& getPool() const;
	~ConnPooler();

      private:
	map<st_mysql*, bool> allConn;
	mutex                allConnMutex;
};

static ConnPooler connPooler;
static int        somethingHappened(MYSQL* mysql, int status);

QString base64this(const char* param) {
	//no alloc o.O
	QByteArray cheap;
	cheap.setRawData(param, strlen(param));
	return base64this(cheap);
}

QString base64this(const QByteArray& param) {
	return QBL("FROM_BASE64('") + param.toBase64() + QBL("')");
}

QString base64this(const QString& param) {
	auto a = param.toUtf8().toBase64();
	return QBL("FROM_BASE64('") + a + QBL("')");
}

QString mayBeBase64(const QString& original) {
	if (original == SQL_NULL) {
		return original;
	} else if (original.isEmpty()) {
		return QSL("''");
	} else {
		return base64this(original);
	}
}

QString base64Nullable(const QString& param) {
	return mayBeBase64(param);
}

sqlRow DB::queryLine(const char* sql) const {
	return queryLine(QByteArray(sql));
}

sqlRow DB::queryLine(const QString& sql) const {
	return queryLine(sql.toUtf8());
}

sqlRow DB::queryLine(const QByteArray& sql) const {
	auto res = query(sql);
	if (res.empty()) {
		return sqlRow();
	}
	return res[0];
}

void DB::setMaxQueryTime(uint time) const {
	query(QSL("SET @@max_statement_time  = %1").arg(time));
}

sqlResult DB::query(const QString& sql) const {
	return query(sql.toUtf8());
}

sqlResult DB::query(const QByteArray& sql) const {
	if (sql.isEmpty()) {
		return sqlResult();
	}
	auto conn = getConn();
	if (conn == nullptr) {
		throw QSL("This mysql instance is not connected! \n") + QStacker16();
	}

	SQLLogger sqlLogger(sql, conf.logError, this);
	if (sql != "SHOW WARNINGS") {
		lastSQL          = sql;
		sqlLogger.logSql = conf.logSql;
	} else {
		sqlLogger.logSql = false;
	}

	pingCheck(conn, sqlLogger);

	{
		QElapsedTimer timer;
		timer.start();

		sharedState.busyConnection++;
		mysql_query(conn, sql.constData());
		sharedState.busyConnection--;
		state.get().queryExecuted++;
		sqlLogger.serverTime = timer.nsecsElapsed();
	}
	if (auto error = mysql_errno(conn); error) {
		switch (error) {
		case 1065:
			//well an empty query is bad, but not too much!
			qWarning().noquote() << "empty query (or equivalent for) " << sql << "in" << QStacker16();
			return sqlResult();

		case 2013: { //conn lost
			//This is sometimes happening, and I really have no idea how to fix, there is already the ping at the beginning, but looks like is not working...
			//so we try to get some info

			auto err = QSL("Mysql error for %1 \nerror was %2 code: %3, connInfo: %4, \n thread: %5,"
			               " queryDone: %6, reconnection: %7, busyConn: %8, totConn: %9, queryTime: %10 (%11)")
			               .arg(QString(sql))
			               .arg(mysql_error(conn))
			               .arg(error)
			               .arg(conf.getInfo())
			               .arg(mysql_thread_id(conn))
			               .arg(state.get().queryExecuted)
			               .arg(state.get().reconnection)
			               .arg(sharedState.busyConnection)
			               .arg(connPooler.getPool().size())
			               .arg((double)sqlLogger.serverTime, 0, 'G', 3)
			               .arg(sqlLogger.serverTime);
			sqlLogger.error = err;

			qWarning().noquote() << err << QStacker16();

			//force again reconnection ...
			closeConn();
			conn = getConn();

			cxaNoStack = true;
			cxaLevel   = CxaLevel::none;
			throw err;
		} break;
		default:
			auto err        = QSL("Mysql error for %1 \nerror was %2 code: %3").arg(QString(sql)).arg(mysql_error(conn)).arg(error);
			sqlLogger.error = err;
			//this line is needed for proper email error reporting
			qWarning().noquote() << err << QStacker16();
			cxaNoStack = true;
			throw err;
		}
	}

	if (noFetch) {
		return sqlResult();
	}
	return fetchResult(&sqlLogger);
}

sqlResult DB::queryCache(const QString& sql, bool on, QString name, uint ttl) {
	mkdir("cachedSQL");

	static std::mutex            lock;
	std::scoped_lock<std::mutex> scoped(lock);

	if (name.isEmpty()) {
		name = "cachedSQL/" + sha1(sql);
	}
	sqlResult res;
	if (on) {
		if (fileUnSerialize(name, res).fileExists) {
			return res;
		}
	}

	lock.unlock();
	res = query(sql);
	lock.lock();
	fileSerialize(name, res);
	return res;
}

sqlResult DB::queryDeadlockRepeater(const QByteArray& sql, uint maxTry) const {
	sqlResult result;
	if (!sql.isEmpty()) {
		for (uint tryNum = 0; tryNum < maxTry; ++tryNum) {
			try {
				return query(sql);
			} catch (unsigned int error) {
				switch (error) {
				case MyError::noError:
					return result;
					break;
				case MyError::deadlock:
					continue;
					break;
				default:;
					throw error;
				}
			}
		}
		qWarning().noquote() << "too many trial to resolve deadlock, fix your code!" + QStacker16();
		cxaNoStack = true;
		throw MyError::deadlock;
	}
	return result;
}

void DB::pingCheck(st_mysql*& conn, SQLLogger& sqlLogger) const {
	auto oldConnId = mysql_thread_id(conn);

	auto guard = qScopeGuard([&] {
		auto newConnId = mysql_thread_id(conn);
		if (oldConnId != newConnId) {
			state.get().reconnection++;
			qDebug() << "detected mysql reconnection";
		}
	});
	//can be disabled in local host to run a bit faster on laggy connection
	if (!conf.pingBeforeQuery) {
		return;
	}
	int connRetry = 0;
	//Those will not emit an error, only the last one
	for (; connRetry < 5; connRetry++) {
		if (mysql_ping(conn)) { //1 on error, which should not even happen ... but here we are
			//force reconnection
			closeConn();
			conn = getConn();
		} else {
			return;
		}
	}
	//last ping check
	if (mysql_ping(conn)) { //1 on error
		auto error = mysql_errno(conn);
		auto err   = QSL("Mysql error for %1 \nerror was %2 code: %3, connRetry for %4, connectionId: %5, conf: ")
		               .arg(QString(sqlLogger.sql))
		               .arg(mysql_error(conn))
		               .arg(error)
		               .arg(connRetry)
		               .arg(mysql_error(conn)) +
		           conf.getInfo() +
		           QStacker16();
		sqlLogger.error = err;
		//this line is needed for proper email error reporting
		qWarning().noquote() << err;
		cxaNoStack = true;
		throw err;
	}
	return;
}

QString DB::escape(const QString& what) const {
	auto plain = what.toUtf8();
	//Ma esiste una lib in C++ per mysql ?
	char* tStr = new char[plain.size() * 2 + 1];
	mysql_real_escape_string(getConn(), tStr, plain.constData(), plain.size());
	auto escaped = QString::fromUtf8(tStr);
	delete[] tStr;
	return escaped;
}

QString QV(const sqlRow& line, const QByteArray& b) {
	return line.value(b);
}

st_mysql* DB::getConn() const {
	st_mysql* curConn = connPool;
	if (curConn == nullptr) {
		//loading in connPool is inside
		curConn = connect();
	}
	return curConn;
}

ulong DB::lastId() const {
	return mysql_insert_id(getConn());
}

const DBConf DB::getConf() const {
	if (!confSet) {
		cxaNoStack = true;
		throw QSL("you have not set the configuration!") + QStacker16();
	}
	return conf;
}

void DB::setConf(const DBConf& value) {
	conf    = value;
	confSet = true;
	for (auto& rx : conf.warningSuppression) {
		rx.optimize();
	}
}

long DB::getAffectedRows() const {
	return affectedRows;
}

DBConf::DBConf() {
}

QByteArray DBConf::getDefaultDB() const {
	if (defaultDB.isEmpty()) {
		auto msg = QSL("default DB is sadly required to avoid mysql complain on certain operation!");
		qWarning().noquote() << msg;
		throw msg;
	}
	return defaultDB;
}

void DBConf::setDefaultDB(const QByteArray& value) {
	defaultDB = value;
}

QString DBConf::getInfo(bool passwd) const {
	auto msg = QSL(" %1:%2  user: %3")
	               .arg(QString(host))
	               .arg(port)
	               .arg(user.data());
	if (passwd) {
		msg += pass.data();
	}
	return msg;
}

DB::DB(const DBConf& conf) {
	setConf(conf);
}

DB::~DB() {
	//will be later removed by the connPooler
	//closeConn();
}

/**
 * @brief DB::closeConn should be called if you know the db instance is been used in a thread (and ofc will not be used anymore)
 * is not a problem if not done, it will just leave a few warn in the error log likeF
 * [Warning] Aborted connection XXX to db: 'ZYX' user: '123' host: 'something' (Got an error reading communication packets)
 */
void DB::closeConn() const {
	st_mysql* curConn = connPool;
	if (curConn) {
		mysql_close(curConn);
		connPooler.removeConn(curConn);
		connPool = nullptr;
	}
}

st_mysql* DB::connect() const {
	//Mysql connection stuff is not thread safe!
	{
		static std::mutex           mutex;
		std::lock_guard<std::mutex> lock(mutex);
		st_mysql*                   conn = mysql_init(nullptr);

		my_bool trueNonSense = 1;
		//looks like is not working very well
		mysql_options(conn, MYSQL_OPT_RECONNECT, &trueNonSense);
		//This will enable non blocking capability
		mysql_options(conn, MYSQL_OPT_NONBLOCK, 0);
		//sensibly speed things up
		mysql_options(conn, MYSQL_OPT_COMPRESS, &trueNonSense);
		//just spam every where to be sure is used
		mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8");
		//Default timeout during connection and operation is Infinite o.O
		//In a real worild if after 5 sec we still have no conn, is clearly an error!
		/*
		uint oldTimeout, readTimeout, writeTimeout;
		mysql_get_option(conn, MYSQL_OPT_CONNECT_TIMEOUT, &oldTimeout);
		mysql_get_option(conn, MYSQL_OPT_READ_TIMEOUT, &readTimeout);
		mysql_get_option(conn, MYSQL_OPT_WRITE_TIMEOUT, &writeTimeout);
		*/

		uint timeout = 10;
		mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
		//Else during long query you will have error 2013
		//mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &timeout);
		mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, &timeout);

		if (!conf.caCert.isEmpty()) {
			mysql_ssl_set(conn, nullptr, nullptr, nullptr, conf.caCert.constData(), nullptr);
		}

		getConf();
		//For some reason mysql is now complaining of not having a DB selected... just select one and gg
		auto connected = mysql_real_connect(conn, conf.host, conf.user.constData(), conf.pass.constData(),
		                                    conf.getDefaultDB(),
		                                    conf.port, conf.sock.constData(), CLIENT_MULTI_STATEMENTS);
		if (connected == nullptr) {
			auto msg = QSL("Mysql connection error (mysql_init). for %1 \n Error %2")
			               .arg(conf.getInfo())
			               .arg(mysql_error(conn));
			mysql_close(conn);
			throw DBException(msg, DBException::Error::Connection);
		}

		/***/
		connPool = conn;
		connPooler.addConnPool(conn);
		/***/
	}

	query(QBL("SET @@SQL_MODE = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"));
	query(QBL("SET time_zone='UTC'"));

	return connPool;
}

bool DB::tryConnect() const {
	try {
		//In try connect. connection error are now very bad...
		cxaLevel = CxaLevel::debug;
		connect();
		return true;
	} catch (...) {
		return false;
	}
}

quint64 getId(const sqlResult& res) {
	if (!res.isEmpty()) {
		auto& line = res.at(0);
		auto  iter = line.find(QBL("last_id"));
		if (iter != line.end()) {
			bool ok = false;
			auto v2 = iter->toULongLong(&ok);
			if (v2 > 0 && ok) {
				return v2;
			}
		}
	}
	qWarning().noquote() << "error fetching last_id" << QStacker16();
	return 0;
}

SQLBuffering::SQLBuffering(DB* _conn, uint _bufferSize) {
	conn       = _conn;
	bufferSize = _bufferSize;
}

SQLBuffering::~SQLBuffering() {
	flush();
}

void SQLBuffering::append(const QString& sql) {
	buffer.append(sql);
	if (sql.isEmpty()) {
		return;
	}
	//0 disable flushing, 1 disable buffering
	if (bufferSize && (uint)buffer.size() >= bufferSize) {
		flush();
	}
}

void SQLBuffering::flush() {
	if (buffer.isEmpty()) {
		return;
	}
	if (conn == nullptr) {
		throw QSL("you forget to set a usable DB Conn!") + QStacker16();
	}
	/**
	 * To avoid having a very big packet we split
	 * usually max_allowed_packet is https://mariadb.com/kb/en/server-system-variables/#max_allowed_packet
	   16777216 (16M) >= MariaDB 10.2.4
		4194304 (4M) >= MariaDB 10.1.7
		1048576 (1MB) <= MariaDB 10.1.6
	 1073741824 (1GB) (client-side)
	 It should be nice to read the value from the conn ... ?
	 TODO add
	 show variables like "max_allowed_packet"
	 */

	//This MUST be out of the buffered block!
	if (useTRX) {
		conn->query(QBL("START TRANSACTION;"));
	}

	QString query;
	//TODO just compose the query in utf8, and append in utf8
	for (auto&& line : buffer) {
		query.append(line);
		query.append(QSL("\n"));
		//this is UTF16, but MySQL run in UTF8, so can be lower or bigger (rare vey rare but possible)
		//small safety margin + increase size for UTF16 -> UTF8 conversion
		if ((query.size() * 1.3) > maxPacket * 0.75) {
			conn->queryDeadlockRepeater(query.toUtf8());
			query.clear();
		}
	}
	if (!query.isEmpty()) {
		conn->queryDeadlockRepeater(query.toUtf8());
	}
	//This MUST be out of the buffered block!
	if (useTRX) {
		conn->query(QBL("COMMIT;"));
	}
	buffer.clear();
}

void SQLBuffering::setUseTRX(bool useTRX) {
	this->useTRX = useTRX;
}

void SQLBuffering::clear() {
	buffer.clear();
}

QString Q64(const sqlRow& line, const QByteArray& b) {
	return base64this(QV(line, b));
}

QByteArray Q8(const sqlRow& line, const QByteArray& b) {
	return line.value(b);
}

sqlResult DB::query(const char* sql) const {
	return query(QByteArray(sql));
}

bool DB::isSSL() const {
	auto res = query("SHOW STATUS LIKE 'Ssl_cipher'");
	if (res.isEmpty()) {
		return false;
	} else {
		auto cypher = res[0]["Value"];
		//whatever is set means is ok
		return cypher.length() > 5;
	}
}

void DB::startQuery(const QByteArray& sql) const {
	int  err;
	auto conn  = getConn();
	signalMask = mysql_real_query_start(&err, conn, sql.constData(), sql.length());
	if (!signalMask) {
		throw QSL("Error executing ASYNC query (start):") + mysql_error(conn);
	}
}

void DB::startQuery(const QString& sql) const {
	return startQuery(sql.toUtf8());
}

void DB::startQuery(const char* sql) const {
	return startQuery(QByteArray(sql));
}

bool DB::completedQuery() const {
	auto conn = getConn();

	auto error = mysql_errno(conn);
	if (error != 0) {
		qWarning().noquote() << "Mysql error for " << lastSQL << "error was " << mysql_error(conn) << " code: " << error << QStacker(3);
		throw 1025;
	}
	int err;

	auto event = somethingHappened(conn, signalMask);
	if (event) {
		event = mysql_real_query_cont(&err, conn, event);
		if (err) {
			throw QSL("Error executing ASYNC query (cont):") + mysql_error(conn);
		}
		//if we are still listening to an event, return false
		//else if we have no more event to wait return true
		return !event;
	} else {
		return false;
	}
}

sqlResult DB::getWarning(bool useSuppressionList) const {
	sqlResult ok;
	auto      warnCount = mysql_warning_count(getConn());
	if (!warnCount) {
		return ok;
	}
	auto res = query(QBL("SHOW WARNINGS"));
	if (!useSuppressionList || conf.warningSuppression.isEmpty()) {
		return res;
	}
	for (auto iter = res.begin(); iter != res.end(); ++iter) {
		auto msg = iter->value(QBL("Message"), BSQL_NULL);
		for (auto rx : conf.warningSuppression) {
			auto p = rx.pattern();
			if (rx.match(msg).hasMatch()) {
				break;
			} else {
				ok.append(*iter);
			}
		}
	}
	return ok;
}

sqlResult DB::fetchResult(SQLLogger* sqlLogger) const {
	QElapsedTimer timer;
	timer.start(); //this will be stopped in the destructor of sql logger
	//most inefficent way, but most easy to use!
	sqlResult res;
	res.reserve(512);

	if (sqlLogger) {
		sqlLogger->res = &res;
	}

	auto conn = getConn();
	//If you batch more than two select, you are crazy, just the first one will be returned and you will be in bad situation later
	//this iteration is just if you batch mulitple update, result is NULL, but mysql insist that you fetch them...
	do {
		//swap the whole result set we do not expect 1Gb+ result set here
		MYSQL_RES* result = mysql_store_result(conn);

		if (result != nullptr) {
			my_ulonglong row_count = mysql_num_rows(result);
			for (uint j = 0; j < row_count; j++) {
				MYSQL_ROW    row        = mysql_fetch_row(result);
				auto         num_fields = mysql_num_fields(result);
				MYSQL_FIELD* fields     = mysql_fetch_fields(result);
				sqlRow       thisItem;
				auto         lengths = mysql_fetch_lengths(result);
				for (uint16_t i = 0; i < num_fields; i++) {
					//this is how sql NULL is signaled, instead of having a wrapper and check ALWAYS before access, we normally just ceck on result swap if a NULL has any sense here or not.
					//Plus if you have the string NULL in a DB you are really looking for trouble
					if (row[i] == nullptr && lengths[i] == 0) {
						thisItem.insert(fields[i].name, BSQL_NULL);
					} else {
						thisItem.insert(fields[i].name, QByteArray(row[i], static_cast<int>(lengths[i])));
					}
				}
				res.push_back(thisItem);
			}
			mysql_free_result(result);
		}
	} while (mysql_next_result(conn) == 0);
	sqlLogger->fetchTime = timer.nsecsElapsed();

	affectedRows = mysql_affected_rows(conn);

	//auto affected  = mysql_affected_rows(conn);
	if (skipWarning) {
		//reset
		skipWarning = false;
	} else {
		auto warn = this->getWarning(true);
		if (!warn.isEmpty()) {
			qDebug().noquote() << "warning for " << lastSQL << warn << "\n"
			                   << QStacker16Light();
		}
	}

	unsigned int error = mysql_errno(conn);
	if (error && sqlLogger) {
		sqlLogger->error = mysql_error(conn);
	}
	if (error) {
		qWarning().noquote() << "Mysql error for " << lastSQL << "error was " << mysql_error(conn) << " code: " << error << QStacker(3);
		cxaNoStack = true;
		throw error;
	}

	return res;
}

int DB::fetchAdvanced(FetchVisitor* visitor) const {
	auto conn = getConn();

	//swap the whole result set we do not expect 1Gb+ result set here
	MYSQL_RES* result = mysql_use_result(conn);
	if (!result) {
		auto error = mysql_errno(conn);
		if (error != 0) {
			qWarning().noquote() << "Mysql error for " << lastSQL << "error was " << mysql_error(conn) << " code: " << error << QStacker(3);
			cxaNoStack = true;
			throw 1025;
		}
	}
	if (!visitor->preCheck(result)) {
		//???
		throw QSL("no idea what to do whit this result set!");
	}
	while (auto row = mysql_fetch_row(result)) {
		visitor->processLine(row);
	}
	mysql_free_result(result);
	//no idea what to return
	return 1;
}

/**
  why static ? -> https://stackoverflow.com/a/15235626/1040618
  in short is not exported
 * @brief wait_for_mysql
 * @param mysql
 * @param status
 * @return
 */
static int somethingHappened(MYSQL* mysql, int status) {
	struct pollfd pfd;
	int           res;

	pfd.fd = mysql_get_socket(mysql);
	pfd.events =
	    (status & MYSQL_WAIT_READ ? POLLIN : 0) |
	    (status & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
	    (status & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);

	//We have no reason to wait, either is ready or not
	res = poll(&pfd, 1, 0);
	if (res == 0)
		return 0;
	else if (res < 0) {
		return 0;
	} else {
		int status = 0;
		if (pfd.revents & POLLIN)
			status |= MYSQL_WAIT_READ;
		if (pfd.revents & POLLOUT)
			status |= MYSQL_WAIT_WRITE;
		if (pfd.revents & POLLPRI)
			status |= MYSQL_WAIT_EXCEPT;
		return status;
	}
}

SQLLogger::SQLLogger(const QByteArray& _sql, bool _enabled, const DB* _db)
    : sql(_sql), logError(_enabled), db(_db) {
}

void SQLLogger::flush() {
	if (flushed) {
		return;
	}
	if (!(logError || logSql)) {
		return;
	}
	flushed = true;
	static std::mutex            lock;
	std::scoped_lock<std::mutex> scoped(lock);

	//we keep open a file and just append from now on...
	//for the moment is just a single file later... who knows
	static QFile file;
	if (!file.isOpen()) {
		file.setFileName("sql.log");
		if (!file.open(QIODevice::WriteOnly | QIODevice::Append)) {
			qWarning().noquote() << "impossible to open sql.log";
			return;
		}
	}

	QDateTime myDateTime = QDateTime::currentDateTime();
	QString   time       = myDateTime.toString(Qt::ISODateWithMs);
	file.write(time.toUtf8() + "\n");

	auto    pid           = getpid();
	auto    mysqlThreadId = mysql_thread_id(db->getConn());
	QString info          = QSL("PID: %1, MySQL Thread: %2 \n").arg(pid).arg(mysqlThreadId);

	file.write(info.toUtf8());

	double     query = serverTime / 1E9;
	double     fetch = fetchTime / 1E9;
	QByteArray buff  = "Query: " + QByteArray::number(query, 'E', 3);
	file.write(buff.leftJustified(20, ' ').append("Fetch: " + QByteArray::number(fetch, 'E', 3)) + "\n" + sql);
	if (!error.isEmpty()) {
		file.write(QBL("\nError: ") + error.toUtf8());
		if (res && !res->isEmpty()) {
			file.write("\n");
			//nice trick to use qDebug operator << on a custom stream!
			QDebug dbg(&file);
			dbg << (*res);
		}
	}

	file.write("\n-------------\n");
	file.flush();
}

SQLLogger::~SQLLogger() {
	flush();
}

QString nullOnZero(uint v) {
	if (v) {
		return QString::number(v);
	} else {
		return SQL_NULL;
	}
}

/**
 * to check reconnection
 * 	while (true) {
		try {
			qDebug() << s7Db.queryLine("SELECT NOW(6)");	
		} catch (...) {
			qDebug() << "Query error";
		}
		
		usleep(1E5);
	}
	
	exit(1);
	*/

Runnable::Runnable(const DBConf& conf) {
	setConf(conf);
}

void Runnable::setConf(const DBConf& conf) {
	db.setConf(conf);
	//this will check if we have the proper table and column available in the selected DB
	try {
		auto row = db.queryLine("SELECT id, operationCode FROM runnable ORDER BY lastRun DESC LIMIT 1");
	} catch (QString e) {
		QString msg = R"(
Is probably missing the runnable table in the db %1, create it with
CREATE TABLE `runnable` (
	`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
	`operationCode` varchar(65000) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
	`lastRun` int(10) unsigned NOT NULL,
	`orario` datetime GENERATED ALWAYS AS (from_unixtime(`lastRun`)) VIRTUAL,
	PRIMARY KEY (`id`),
KEY `lastRun` (`lastRun`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
					  )";
		throw msg.arg(QString(db.getConf().getDefaultDB()));
	}
}

bool Runnable::runnable(const QString& key, qint64 second) {
	static const QString skel = "SELECT id, lastRun FROM runnable WHERE operationCode = %1 ORDER BY lastRun DESC LIMIT 1";
	auto                 now  = QDateTime::currentSecsSinceEpoch();
	auto                 sql  = skel.arg(base64this(key));
	auto                 res  = db.query(sql);
	if (res.isEmpty() or res.at(0).value("lastRun", BZero).toLongLong() + second < now) {
		static const QString skel = "INSERT INTO runnable SET operationCode = %1, lastRun = %2";
		auto                 sql  = skel.arg(base64this(key)).arg(now);
		db.query(sql);

		return true;
	} else {
		return false;
	}
}

QString sqlRow::serialize() const {
	//Almost free operator <<
	QString out;
	QDebug  dbg(&out);
	dbg << (*this);
	return out;
}

void ConnPooler::addConnPool(st_mysql* conn) {
	lock_guard<mutex> guard(allConnMutex);
	allConn.insert({conn, true});
}

void ConnPooler::removeConn(st_mysql* conn) {
	lock_guard<mutex> guard(allConnMutex);
	if (auto iter = allConn.find(conn); iter != allConn.end()) {
		allConn.erase(iter);
	}
}

void ConnPooler::closeAll() {
	lock_guard<mutex> guard(allConnMutex);
	for (auto& [conn, dummy] : allConn) {
		mysql_close(conn);
	}
	allConn.clear();
}

const map<st_mysql*, bool>& ConnPooler::getPool() const {
	return allConn;
}

ConnPooler::~ConnPooler() {
	closeAll();
}

DBException::DBException(const QString& _msg, Error error) {
	msg       = _msg;
	msg8      = msg.toUtf8();
	errorType = error;
}

const char* DBException::what() const noexcept {
	return msg8.constData();
}
