#include "min_mysql.h"
#include "mysql/mysql.h"
#include <QDataStream>
#include <QDateTime>
#include <QDebug>
#include <QFile>
#include <QMap>
#include <mutex>

#define QBL(str) QByteArrayLiteral(str)
#define QSL(str) QStringLiteral(str)

QByteArray QStacker(uint skip = 0);

class QFileXT2 : public QFile {
      public:
	bool open(OpenMode flags) override;
};

bool QFileXT2::open(QIODevice::OpenMode flags) {
	if (!QFile::open(flags)) {
		qCritical().noquote() << errorString() << "opening" << fileName() << "\n"
		                      << QStacker();
		return false;
	}
	return true;
}

QString base64this(const char* param) {
	//no alloc o.O
	QByteArray cheap;
	cheap.setRawData(param, strlen(param));
	return base64this(cheap);
}

QString base64this(const QByteArray& param) {
	return "FROM_BASE64('" + param.toBase64() + "')";
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

sqlResult MySQL_query(st_mysql* conn, const QByteArray& sql) {
	return query(conn, sql);
}

sqlResult query(st_mysql* conn, const QString& sql) {
	return query(conn, sql.toUtf8());
}

sqlResult MySQL_query(st_mysql* conn, const QString& sql) {
	return query(conn, sql.toUtf8());
}

sqlResult DB::query(const QString& sql) {
	if (getConn() == nullptr) {
		connect();
	}
	return MySQL_query(this->getConn(), sql);
}

sqlResult DB::query(const QByteArray& sql) {
	if (getConn() == nullptr) {
		connect();
	}
	return MySQL_query(this->getConn(), sql);
}

struct SaveSql {
	SaveSql(const QByteArray& _sql, const sqlResult* _res)
	    : sql(_sql), res(_res) {
	}
	~SaveSql() {
		static std::mutex            lock;
		std::scoped_lock<std::mutex> scoped(lock);

		//we keep open a file and just append from now on...
		//for the moment is just a single file later... who knows
		static QFileXT2 file;
		if (!file.isOpen()) {
			file.setFileName("sql.log");
			if (!file.open(QIODevice::WriteOnly | QIODevice::Append)) {
				return;
			}
		}

		QDateTime myDateTime = QDateTime::currentDateTime();
		QString   time       = myDateTime.toString(Qt::ISODateWithMs);

		file.write(time.toUtf8() + QBL("\nErroCode: ") + QByteArray::number(erroCode) + QBL("\t\t") + sql);
		if (res && !res->isEmpty()) {
			file.write("\n");
			QDebug dbg(&file);
			dbg << (*res);
		}
		file.write("\n--------\n");
		file.flush();
	}
	const QByteArray& sql;
	const sqlResult*  res      = nullptr;
	uint              erroCode = 99999;
};

sqlResult query(st_mysql* conn, const QByteArray& sql) {
	QList<sqlRow> res;
	res.reserve(512);

	SaveSql save(sql, &res);
	if (conn == nullptr) {
		throw QSL("This mysql instance is not connected! \n") + QStacker();
	}

	mysql_query(conn, sql.constData());
	auto error    = mysql_errno(conn);
	save.erroCode = error;

	if (error != 0) {
		auto err = QSL("Mysql error for ") + sql.constData() + QSL("error was ") + mysql_error(conn) + QSL(" code: ") + error;
		throw err;
	}

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
	if (warnCount && !sql.toLower().contains(QBL("drop table if exists"))) {
		qDebug().noquote() << "warning for " << sql << query(conn, QBL("SHOW WARNINGS"));
	}

	error = mysql_errno(conn);
	if (error != 0) {
		qCritical().noquote() << "Mysql error for " << sql.constData() << "error was " << mysql_error(conn) << " code: " << error; // << QStacker(3);
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

QString QV(const QMap<QByteArray, QByteArray>& line, const QByteArray& b) {
	return line.value(b);
}

st_mysql* DB::getConn() {
	if (conn == nullptr) {
		connect();
	}
	return conn;
}

void DB::connect() {
	//Mysql connection stuff is not thread safe!
	static std::mutex           mutex;
	std::lock_guard<std::mutex> lock(mutex);
	if(conn != nullptr){
		return;
	}
	conn = mysql_init(nullptr);

	my_bool reconnect = 1;
	mysql_options(getConn(), MYSQL_OPT_RECONNECT, &reconnect);

	mysql_options(getConn(), MYSQL_SET_CHARSET_NAME, "utf8");
	//	if(!conf().db.certificate.isEmpty()){
	//		mysql_ssl_set(conn,nullptr,nullptr,conf().db.certificate.constData(),nullptr,nullptr);

	//	}
	auto connected = mysql_real_connect(getConn(), host.constData(), user.constData(), pass.constData(),
	                                    nullptr, port, nullptr, CLIENT_MULTI_STATEMENTS);
	if (connected == nullptr) {
		auto msg = QSL("Mysql connection error (mysql_init).") + mysql_error(getConn());
		throw msg;
	}
	query(QBL("SET @@SQL_MODE = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"));
	query(QBL("SET time_zone='UTC'"));

	//For some reason mysql is now complaining of not having a DB selected... just select one and gg
	query("use " + defaultDB);
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
	buffer.append(QSL("START TRANSACTION;"));
}

SQLBuffering::~SQLBuffering() {
	flush();
}

void SQLBuffering::append(const QString& sql) {
	buffer.append(sql);
	if(sql.isEmpty()){
		return;
	}
	if (buffer.size() > bufferSize) {
		flush();
	}
}

void SQLBuffering::flush() {
	if (buffer.isEmpty()) {
		return;
	}
	buffer.append(QSL("COMMIT;"));
	conn->query(buffer.join("\n"));
	buffer.clear();
	buffer.append(QSL("START TRANSACTION;"));
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
