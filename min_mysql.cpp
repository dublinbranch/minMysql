#include "min_mysql.h"
#include "QStacker/qstacker.h"
#include "mysql/mysql.h"
#include <QDataStream>
#include <QDateTime>
#include <QDebug>
#include <QFile>
#include <QMap>
#include <memory>
#include <mutex>
#include <poll.h>

static int somethingHappened(MYSQL* mysql, int status);

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
	} else {
		return base64this(original);
	}
}

sqlResult DB::query(const QString& sql) const {
	return query(sql.toUtf8());
}

sqlResult DB::query(const QByteArray& sql) const {
	auto conn = getConn();
	if (conn == nullptr) {
		throw QSL("This mysql instance is not connected! \n") + QStacker16();
	}

	lastSQL = sql;
	SQLLogger sqlLogger(sql, sqlLoggerON);

	mysql_query(conn, sql.constData());
	auto error = mysql_errno(conn);
	if (error) {
		switch (error) {
		case 1065:
			//well an empty query is bad, but not too much!
			qWarning().noquote() << "empty query (or equivalent for) " << sql << "in" << QStacker16();
			return sqlResult();
		}
		auto err        = QSL("Mysql error for ") + sql.constData() + QSL("error was ") + mysql_error(conn) + QSL(" code: ") + error;
		sqlLogger.error = err;
		//this line is needed for proper email error reporting
		qWarning().noquote() << err << QStacker16();
		cxaNoStack = true;
		throw err;
	}

	if (noFetch) {
		return sqlResult();
	}
	return fetchResult(&sqlLogger);
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

QString QV(const QMap<QByteArray, QByteArray>& line, const QByteArray& b) {
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

/**
 * @brief DB::affectedRows looks broken, it return 1 even if there is nothing inserted o.O
 * @return
 */
long DB::affectedRows() const {
	return mysql_affected_rows(getConn());
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

DB::DB(const DBConf& conf) {
	setConf(conf);
}

DB::~DB() {
	st_mysql* curConn = connPool;
	if (curConn) {
		mysql_close(curConn);
		connPool = nullptr;
	}
}

st_mysql* DB::connect() const {
	//Mysql connection stuff is not thread safe!
	static std::mutex           mutex;
	std::lock_guard<std::mutex> lock(mutex);
	st_mysql*                   conn = mysql_init(nullptr);

	my_bool trueNonSense = 1;
	mysql_options(conn, MYSQL_OPT_RECONNECT, &trueNonSense);
	//This will enable non blocking capability
	mysql_options(conn, MYSQL_OPT_NONBLOCK, 0);
	//sensibly speed things up
	mysql_options(conn, MYSQL_OPT_COMPRESS, &trueNonSense);

	mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8");
	if (!conf.caCert.isEmpty()) {
		mysql_ssl_set(conn, nullptr, nullptr, nullptr, conf.caCert.constData(), nullptr);
	}

	getConf();
	//For some reason mysql is now complaining of not having a DB selected... just select one and gg
	auto connected = mysql_real_connect(conn, conf.host, conf.user.constData(), conf.pass.constData(),
	                                    conf.getDefaultDB(),
	                                    conf.port, conf.sock.constData(), CLIENT_MULTI_STATEMENTS);
	if (connected == nullptr) {
		auto msg = QSL("Mysql connection error (mysql_init). for %1 : %2 ").arg(QString(conf.host)).arg(conf.port) + mysql_error(conn) + QStacker16Light();
		throw msg;
	}

	/***/
	connPool = conn;
	/***/

	query(QBL("SET @@SQL_MODE = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"));
	query(QBL("SET time_zone='UTC'"));

	return conn;
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

SQLBuffering::SQLBuffering(DB* _conn, int _bufferSize) {
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
	if (bufferSize && buffer.size() >= bufferSize) {
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
	conn->query(QSL("START TRANSACTION;"));

	QString query;
	for (auto&& line : buffer) {
		query.append(line);
		query.append(QSL("\n"));
		//this is UTF16, but MySQL run in UTF8, so can be lowet or bigger (rare vey rare but possible)
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
	conn->query(QSL("COMMIT;"));
	buffer.clear();
}

QString Q64(const sqlRow& line, const QByteArray& b) {
	return base64this(QV(line, b));
}

QByteArray Q8(const sqlRow& line, const QByteArray& b) {
	return line.value(b);
}

QString base64Nullable(const QString& param) {
	if (param == SQL_NULL) {
		return param;
	}
	auto a = param.toUtf8().toBase64();
	return QBL("FROM_BASE64('") + a + QBL("')");
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

sqlResult DB::fetchResult(SQLLogger* sqlLogger) const {
	//most inefficent way, but most easy to use!
	sqlResult res;
	res.reserve(512);

	//this is 99.9999% useless and will never again be used
	auto cry = std::shared_ptr<SQLLogger>();
	if (!sqlLogger) {
		cry       = std::make_shared<SQLLogger>(lastSQL, sqlLoggerON);
		sqlLogger = cry.get();
	}
	sqlLogger->res = &res;

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
			return res;
		}
	} while (mysql_next_result(conn) == 0);

	//auto affected  = mysql_affected_rows(conn);
	auto warnCount = mysql_warning_count(conn);
	if (warnCount) {
		qDebug().noquote() << "warning for " << lastSQL << query(QBL("SHOW WARNINGS"));
	}

	unsigned int error = mysql_errno(conn);
	sqlLogger->error   = mysql_error(conn);
	if (error) {
		qWarning().noquote() << "Mysql error for " << lastSQL << "error was " << mysql_error(conn) << " code: " << error << QStacker(3);
		cxaNoStack = true;
		throw error;
	}

	auto v = mysql_insert_id(conn);
	if (v > 0) {
		sqlRow thisItem;
		thisItem.insert(QBL("last_id"), QByteArray::number(v));
		res.push_back(thisItem);
		return res;
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

void SQLLogger::flush() {
	if (flushed) {
		return;
	}
	if (!enabled) {
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

	file.write(time.toUtf8() + QBL("\nError: ") + error.toUtf8() + QBL("\n") + sql);
	if (res && !res->isEmpty()) {
		file.write("\n");
		//nice trick to use qDebug operator << on a custom stream!
		QDebug dbg(&file);
		dbg << (*res);
	}
	file.write("\n--------\n");
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
