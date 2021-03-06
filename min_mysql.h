#pragma once

#include "MITLS.h"
#include "QStacker/qstacker.h"
#include "const.h"
#include "magicEnum/magic_from_string.hpp"
#include "mapExtensor/qmapV2.h"
#include <QDateTime>
#include <QRegularExpression>
#include <QStringList>

#ifndef QBL
#define QBL(str) QByteArrayLiteral(str)
#define QSL(str) QStringLiteral(str)
#endif

enum MyError : unsigned int {
	noError  = 0,
	deadlock = 1213
};

class DBException : public ExceptionV2 {
      public:
	enum Error : int {
		NA = 0,
		Connection,
		Warning,
		SchemaError,
		NoResult
	} errorType = Error::NA;
	DBException(const QString& _msg, Error error);
};

QString base64this(const char* param);
QString base64this(const QByteArray& param);
QString base64this(const QString& param);
QString base64this(const std::string& param);
QString mayBeBase64(const QString& original, bool emptyAsNull = false);
QString base64Nullable(const QString& param, bool emptyAsNull = false);
QString nullOnZero(uint v);

struct st_mysql;
struct st_mysql_res;

class sqlRow : public QMapV2<QByteArray, QByteArray> {
      public:
	template <typename D>
	void rq(const QByteArray& key, D& dest) const {
		QByteArray temp;
		get(key, temp);
		swap(temp, dest);
	}

	QDateTime asDateTime(const QByteArray& key) const {
		auto time = get2<QString>(key);
		return QDateTime::fromString(time, mysqlDateTimeFormat);
	}

	template <typename D>
	[[deprecated("use rq")]] void get2(const QByteArray& key, D& dest) const {
		rq(key, dest);
	}

	//To avoid conversion back and forth QBytearray of the default value and the his result
	template <typename D>
	bool get2(const QByteArray& key, D& dest, const D& def) const {
		if (auto v = this->fetch(key); v) {
			swap(*v.value, dest);
			return true;
		}
		dest = def;
		return false;
	}

	template <typename D>
	D get2(const QByteArray& key) const {
		QByteArray temp;
		D          temp2;
		get(key, temp);
		swap(temp, temp2);
		return temp2;
	}

	//Sooo many time we need a QString back
	QString g16(const QByteArray& key) const {
		return get2<QString>(key);
	}

	//Sooo many time we need a QString back
	QString g16(const QByteArray& key, const QString def) const {
		QString val;
		get2(key, val, def);
		return val;
	}
	//When you can not use operator <<
	QString serialize() const;

      private:
	template <typename D>
	void swap(const QByteArray& source, D& dest) const {
		if constexpr (std::is_same<D, QString>::value) {
			dest = QString(source);
			return;
		} else if constexpr (std::is_same<D, QByteArray>::value) {
			dest = source;
			return;
		} else if constexpr (std::is_same<D, std::string>::value) {
			dest = source.toStdString();
			return;
		} else if constexpr (std::is_same<D, QDate>::value) {
			dest = QDate::fromString(source, mysqlDateFormat);
			return;
		} else if constexpr (std::is_same<D, QDateTime>::value) {
			dest = QDateTime::fromString(source, mysqlDateTimeFormat);
			return;
		} else if constexpr (std::is_enum_v<D>) {
			auto s = source.toStdString();
			magic_enum::fromString(s, dest);
			return;
		} else if constexpr (std::is_arithmetic_v<D>) {
			bool ok = false;
			if constexpr (std::is_floating_point_v<D>) {
				dest = source.toDouble(&ok);
			} else if constexpr (std::is_signed_v<D>) {
				dest = source.toLongLong(&ok);
			} else if constexpr (std::is_unsigned_v<D>) {
				dest = source.toULongLong(&ok);
			}
			if (!ok) {
				//last chanche NULL is 0 in case we are numeric right ?
				if (source == QBL("NULL")) {
					if constexpr (std::is_arithmetic_v<D>) {
						dest = 0;
						return;
					}
				}
				throw QSL("Impossible to convert %1 as a number").arg(QString(source));
			}
		} else {
			//poor man static assert that will also print for which type it failed
			typedef typename D::something_made_up X;

			X y;     //To avoid complain that X is defined but not used
			(void)y; //TO avoid complain that y is unused
		}
	}
};
using sqlResult = QList<sqlRow>;

struct DB;
struct DBConf;

struct SQLLogger {
	SQLLogger(const QByteArray& _sql, bool _enabled, const DB* _db);
	void flush();
	~SQLLogger();

	qint64           serverTime;
	qint64           fetchTime;
	const QByteArray sql;
	const sqlResult* res = nullptr;
	QString          error;
	//TODO questi due leggili dal conf dell db*
	bool logSql   = false;
	bool logError = false;
	bool flushed  = false;
	//the invoking class
	const DB* db = nullptr;
};
//class QRegularExpression;
struct DBConf {
	DBConf();
	QByteArray host = "127.0.0.1";
	QByteArray pass;
	QByteArray user;
	QByteArray sock;
	bool       ssl = false;
	//Usually set false for operation that do not have to be replicated
	bool                      writeBinlog = true;
	QList<QRegularExpression> warningSuppression;
	uint                      port            = 3306;
	bool                      logSql          = false;
	bool                      logError        = false;
	bool                      pingBeforeQuery = true; //So if the connection is broken will be re-established
	//In certain case not beeing able to connect is bad, in other not and we just go ahead, retry later...
	CxaLevel connErrorVerbosity = CxaLevel::none;

	//Corpus munus
	QByteArray getDefaultDB() const;
	void       setDefaultDB(const QByteArray& value);
	QString    getInfo(bool passwd = false) const;

      private:
	QByteArray defaultDB;
};

/**
 * @brief The DB struct
 */
class FetchVisitor;
struct DB {
      public:
	DB() = default;
	DB(const DBConf& _conf);
	~DB();
	void      closeConn() const;
	st_mysql* connect() const;
	bool      tryConnect() const;
	sqlRow    queryLine(const char* sql) const;
	sqlRow    queryLine(const QString& sql) const;
	sqlRow    queryLine(const QByteArray& sql) const;

	void      setMaxQueryTime(uint time) const;
	sqlResult query(const char* sql) const;
	sqlResult query(const QString& sql) const;
	sqlResult query(const QByteArray& sql) const;

	[[deprecated("use queryCache2 - this one is problematic to use, and with redundant and never used param")]] sqlResult  queryCache(const QString& sql, bool on = false, QString name = QString(), uint ttl = 3600);
	[[deprecated("use queryCacheLine2 - this one is problematic to use, and with redundant and never used param")]] sqlRow queryCacheLine(const QString& sql, bool on = false, QString name = QString(), uint ttl = 3600, bool required = false);

	sqlRow queryCacheLine(const QString& sql, uint ttl = 3600, bool required = false);
	sqlRow queryCacheLine2(const QString& sql, uint ttl = 3600, bool required = false);

	sqlResult queryCache2(const QString& sql, uint ttl);

	//This is to be used ONLY in case the query can have deadlock, and internally tries multiple times to insert data
	sqlResult queryDeadlockRepeater(const QByteArray& sql, uint maxTry = 5) const;

	void    pingCheck(st_mysql*& conn, SQLLogger& sqlLogger) const;
	QString escape(const QString& what) const;
	bool    isSSL() const;
	/**
	  Those 2 are used toghether for the ASYNC mode
	 * @brief startQuery
	 * @param sql
	 *
	 */
	void startQuery(const QByteArray& sql) const;
	void startQuery(const QString& sql) const;
	void startQuery(const char* sql) const;
	/** use something like
		while (!db.completedQuery()) {
			usleep(100);
		}
		fetch
	 * @brief completedQuery
	 * @return
	 */
	bool completedQuery() const;

	//Shared by both async and not
	sqlResult getWarning(bool useSuppressionList = true) const;
	sqlResult fetchResult(SQLLogger* sqlLogger = nullptr) const;
	int       fetchAdvanced(FetchVisitor* visitor) const;
	st_mysql* getConn() const;
	ulong     lastId() const;

	//Non copyable
	DB& operator=(const DB&) = delete;
	DB(const DB&)            = delete;

	//this will require query + fetchAdvanced
	mutable mi_tls<bool> noFetch = false;
	//JUST For the next query the WARNING spam will be suppressed, use if you understand what you are doing
	mutable mi_tls<bool> skipWarning = false;

	const DBConf getConf() const;
	void         setConf(const DBConf& value);

	long getAffectedRows() const;
	struct InternalState {
		//This will hopefully help track down the disconnection issue
		uint    queryExecuted = 0;
		uint    reconnection  = 0;
		bool    NULL_as_EMPTY = false;
		QString lastError;
	};
	mutable mi_tls<InternalState> state;

      private:
	bool   confSet = false;
	DBConf conf;
	//Mutable is needed for all of them
	mutable mi_tls<long> affectedRows;
	//this allow to spam the DB handler around, and do not worry of thread, each thread will create it's own connection!
	mutable mi_tls<st_mysql*> connPool;
	//used for asyncs
	mutable mi_tls<int>        signalMask;
	mutable mi_tls<QByteArray> lastSQL;
	struct SharedState {
		std::atomic<uint> busyConnection = 0;
	};

	static SharedState sharedState;
};

using MYSQL_ROW = char**;
class FetchVisitor {
      public:
	virtual void processLine(MYSQL_ROW row)     = 0;
	virtual bool preCheck(st_mysql_res* result) = 0;
};

QString    QV(const sqlRow& line, const QByteArray& b);
QByteArray Q8(const sqlRow& line, const QByteArray& b);
QString    Q64(const sqlRow& line, const QByteArray& b);
quint64    getId(const sqlResult& res);

/**
 * @brief The SQLBuffering class it is a set of SQL queries with a flashing system which allows to execute
 * the queries (manually or automatically)
 */
class SQLBuffering {
	// https://mariadb.com/kb/en/server-system-variables/#max_allowed_packet in our system is always 16M atm
	static const uint maxPacket = 16E6;
	//Set as false in case we are running inside another TRX
	bool useTRX = true;

      public:
	DB*         conn       = nullptr;
	uint        bufferSize = 1000;
	QStringList buffer;
	/**
	 * @brief SQLBuffering
	 * @param _conn
	 * @param _bufferSize 0 disable auto flushing, 1 disable buffering
	 */
	SQLBuffering(DB* _conn, uint _bufferSize = 1000);
	SQLBuffering() = default;
	~SQLBuffering();
	void append(const QString& sql);
	void flush();
	void setUseTRX(bool _useTRX);
	void clear();
};

class Runnable {
      public:
	//Non copyable
	Runnable& operator=(const Runnable&) = delete;
	Runnable(const Runnable&)            = delete;
	Runnable()                           = default;
	Runnable(const DBConf& conf);
	void setConf(const DBConf& conf);
	/**
	 * @brief coolDown will check the last time a "key" has been activated
	 * @param key
	 * @param time
	 * @return true: we can run, false: do not run
	 */
	[[nodiscard]] bool runnable(const QString& key, qint64 second);

      private:
	DB db;
};
