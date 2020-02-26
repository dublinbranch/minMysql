#include "min_mysql.h"
#include "mysql/mysql.h"
#include <QDataStream>
#include <QDateTime>
#include <QDebug>
#include <QFile>
#include <QMap>
#include <mutex>
#include <poll.h>

#define QBL(str) QByteArrayLiteral(str)
#define QSL(str) QStringLiteral(str)

static int somethingHappened(MYSQL* mysql, int status);

QByteArray QStacker(uint skip = 0);

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
		throw QSL("This mysql instance is not connected! \n") + QStacker();
	}

	lastSQL = sql;
	SQLLogger sqlLogger(sql, saveQuery);

	mysql_query(conn, sql.constData());
	auto error = mysql_errno(conn);
	if (error != 0) {
		auto err        = QSL("Mysql error for ") + sql.constData() + QSL("error was ") + mysql_error(conn) + QSL(" code: ") + error;
		sqlLogger.error = err;
		//this line is needed for proper email error reporting
		qCritical() << err;
		throw err;
	}

	return fetchResult(&sqlLogger);
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

QString DB::getDefaultDB() const {
	if (defaultDB.isEmpty()) {
		auto msg = QSL("default DB is sadly required to havoid mysql complain on certain operation!");
		qCritical() << msg;
		throw msg;
	}
	return defaultDB;
}

void DB::setDefaultDB(const QString& value) {
	defaultDB = value;
}

st_mysql* DB::connect() const {
	//Mysql connection stuff is not thread safe!
	static std::mutex           mutex;
	std::lock_guard<std::mutex> lock(mutex);
	st_mysql*                   conn = mysql_init(nullptr);

	my_bool trueNonSense = 1;
	mysql_options(conn, MYSQL_OPT_RECONNECT, &trueNonSense );
	//This will enable non blocking capability
	mysql_options(conn, MYSQL_OPT_NONBLOCK, 0);
	//sensibly speed things up
	mysql_options(conn, MYSQL_OPT_COMPRESS, &trueNonSense);

	mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8");
	//	if(!conf().db.certificate.isEmpty()){
	//		mysql_ssl_set(conn,nullptr,nullptr,conf().db.certificate.constData(),nullptr,nullptr);

	//	}
	auto connected = mysql_real_connect(conn, host.constData(), user.constData(), pass.constData(),
	                                    nullptr, port, nullptr, CLIENT_MULTI_STATEMENTS);
	if (connected == nullptr) {
		auto msg = QSL("Mysql connection error (mysql_init).") + mysql_error(conn);
		throw msg;
	}

	/***/
	connPool = conn;
	/***/
	query(QBL("SET @@SQL_MODE = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"));
	query(QBL("SET time_zone='UTC'"));

	//For some reason mysql is now complaining of not having a DB selected... just select one and gg
	query("use " + getDefaultDB());
	return conn;
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
	qCritical().noquote() << "error fetching last_id" << QStacker();
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
		throw QSL("you forget to set a usable DB Conn!") + QStacker();
	}
	buffer.append(QSL("COMMIT;"));
	buffer.prepend(QSL("START TRANSACTION;"));
	conn->query(buffer.join("\n"));
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
	int  err;
	auto conn  = getConn();
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
		cry       = std::make_shared<SQLLogger>(lastSQL, saveQuery);
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

	auto error       = mysql_errno(conn);
	sqlLogger->error = mysql_error(conn);
	if (error != 0) {
		qCritical().noquote() << "Mysql error for " << lastSQL.constData() << "error was " << mysql_error(conn) << " code: " << error; // << QStacker(3);
		throw 1025;
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

int DB::fetchAdvanced(FetchVisitor *visitor) const {
	auto conn = getConn();

	//swap the whole result set we do not expect 1Gb+ result set here
	MYSQL_RES* result = mysql_use_result(conn);
	if (!result) {
		auto error       = mysql_errno(conn);
		if (error != 0) {
			qCritical().noquote() << "Mysql error for " << lastSQL.constData() << "error was " << mysql_error(conn) << " code: " << error; // << QStacker(3);
			throw 1025;
		}
	}
	if(!visitor->preCheck(result)){
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
	flushed = true;
	static std::mutex            lock;
	std::scoped_lock<std::mutex> scoped(lock);

	//we keep open a file and just append from now on...
	//for the moment is just a single file later... who knows
	static QFile file;
	if (!file.isOpen()) {
		file.setFileName("sql.log");
		if (!file.open(QIODevice::WriteOnly | QIODevice::Append)) {
			qCritical() << "impossible to open sql.log";
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


