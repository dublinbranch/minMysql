#ifndef MIN_MYSQL_H
#define MIN_MYSQL_H

#include "MITLS.h"
#include <QStringList>

//Those variable are shared in many places, order of initialization is important!
//Inline will avoid to have multiple copy, and enforces having a single one
inline const QString    mysqlDateFormat = "yyyy-MM-dd";
inline const QString    SQL_NULL        = "NULL";
inline const QByteArray BSQL_NULL       = "NULL";
inline const QByteArray BZero           = "0";
inline const QString    Zero            = "0";

QString base64this(const char* param);
QString base64this(const QByteArray& param);
QString base64this(const QString& param);
QString mayBeBase64(const QString& original);
QString base64Nullable(const QString& param);

struct st_mysql;
using sqlRow    = QMap<QByteArray, QByteArray>;
using sqlResult = QList<sqlRow>;
sqlResult MySQL_query(st_mysql* conn, const QByteArray& sql);
sqlResult MySQL_query(st_mysql* conn, const QString& sql);
sqlResult query(st_mysql* conn, const QString& sql);
sqlResult query(st_mysql* conn, const QByteArray& sql);

struct DB {
      public:
	QByteArray host = "127.0.0.1";
	QByteArray pass;
	QByteArray user;
	QString    defaultDB;
	uint       port = 3306;
	st_mysql*  connect();
	sqlResult  query(const QString& sql);
	sqlResult  query(const QByteArray& sql);
	sqlResult  query(const char* sql);
	st_mysql*  getConn();
	ulong      lastId();

	//Non copyable
	DB()        = default;
	DB& operator=(const DB&) = delete;
	DB(const DB&)            = delete;

      private:
	//Each thread and each instance will need it's own copy
	mi_tls<st_mysql*> connPool;
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

      public:
	/**
	 * @brief SQLBuffering
	 * @param _conn
	 * @param _bufferSize 0 disable flushing, 1 disable buffering
	 */
	SQLBuffering(DB* _conn, int _bufferSize = 1000);
	SQLBuffering() = default;
	~SQLBuffering();
	void append(const QString& sql);
	void flush();
};
#endif // MIN_MYSQL_H
