#ifndef MIN_MYSQL_H
#define MIN_MYSQL_H

#include "MITLS.h"
#include <QStringList>


#ifndef QBL
#define QBL(str) QByteArrayLiteral(str)
#define QSL(str) QStringLiteral(str)
#endif

enum MyError : unsigned int {
	noError  = 0,
	deadlock = 1213
};

//Those variable are shared in many places, order of initialization is important!
//Inline will avoid to have multiple copy, and enforces having a single one
inline const QString    mysqlDateFormat     = "yyyy-MM-dd";
inline const QString    mysqlDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
inline const QString    SQL_NULL            = "NULL";
inline const QByteArray BSQL_NULL           = "NULL";
inline const QByteArray BZero               = "0";
inline const QString    Zero                = "0";

QString base64this(const char* param);
QString base64this(const QByteArray& param);
QString base64this(const QString& param);
QString mayBeBase64(const QString& original);
QString base64Nullable(const QString& param);
QString nullOnZero(uint v);

struct st_mysql;
struct st_mysql_res;
using sqlRow    = QMap<QByteArray, QByteArray>;
using sqlResult = QList<sqlRow>;

struct SQLLogger {
	SQLLogger(const QByteArray& _sql, bool _enabled)
	    : sql(_sql), logError(_enabled) {
	}
	SQLLogger() = default;
	void flush();
	~SQLLogger();

	qint64           serverTime;
	qint64           fetchTime;
	const QByteArray sql;
	const sqlResult* res = nullptr;
	QString          error;
	bool             logSql   = false;
	bool             logError = false;
	bool             flushed  = false;
};

struct DBConf {
	QByteArray host = "127.0.0.1";
	QByteArray pass;
	QByteArray user;
	QByteArray sock;
	QByteArray caCert;

	uint port     = 3306;
	bool logSql   = false;
	bool logError = false;

	QByteArray getDefaultDB() const;
	void       setDefaultDB(const QByteArray& value);

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
	DB(const DBConf& conf);
	~DB();
	st_mysql* connect() const;
	bool      tryConnect() const;
	sqlResult query(const QString& sql) const;
	sqlResult query(const QByteArray& sql) const;
	//This is to be used ONLY in case the query can have deadlock, and internally tries multiple times to insert data
	sqlResult queryDeadlockRepeater(const QByteArray& sql, uint maxTry = 5) const;
	sqlResult query(const char* sql) const;
	bool      isSSL() const;
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
	sqlResult fetchResult(SQLLogger* sqlLogger = nullptr) const;
	int       fetchAdvanced(FetchVisitor* visitor) const;
	st_mysql* getConn() const;
	ulong     lastId() const;
	long      affectedRows() const;

	//Non copyable
	DB& operator=(const DB&) = delete;
	DB(const DB&)            = delete;

	//this will require query + fetchAdvanced
	mutable bool noFetch = false;

	const DBConf getConf() const;
	void         setConf(const DBConf& value);

      private:
	bool   confSet = false;
	DBConf conf;
	//this allow to spam the DB handler around, and do not worry of thread, each thread will create it's own connection!
	mutable mi_tls<st_mysql*> connPool;
	//user for asyncs
	mutable mi_tls<int>        signalMask;
	mutable mi_tls<QByteArray> lastSQL;
	//The value is not RESETTED if the last query do not use a insert, IE if you do a select after an insert it will still be there!
	mutable mi_tls<unsigned long long> lastIdval;
	
};

typedef char** MYSQL_ROW;
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
	DB*         conn = nullptr;
	QStringList buffer;
	int         bufferSize = 1000;
	// https://mariadb.com/kb/en/server-system-variables/#max_allowed_packet in our system is always 16M atm
	uint maxPacket = 16E6;

      public:
	/**
	 * @brief SQLBuffering
	 * @param _conn
	 * @param _bufferSize 0 disable auto flushing, 1 disable buffering
	 */
	SQLBuffering(DB* _conn, int _bufferSize = 1000);
	SQLBuffering() = default;
	~SQLBuffering();
	void append(const QString& sql);
	void flush();
};

#endif // MIN_MYSQL_H
