package org.fao.ess.d3s.cache.postgres;

import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.Page;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.d3s.cache.dto.StoreStatus;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.Type;
import org.fao.fenix.d3s.cache.storage.StorageName;

import javax.inject.Singleton;
import java.sql.*;
import java.util.*;
import java.util.Date;

@Singleton
@StorageName("postgres")
public class DefaultStorage extends PostgresStorage {

    private static String SCHEMA_NAME = "DATA";
    private final Map<String, Connection> session = new HashMap<>();


    //SESSION
    @Override
    public synchronized Connection beginSession(String tableName) throws Exception {
        if (session.containsKey(tableName))
            return session.get(tableName);

        Connection connection = getConnection();
        connection.setAutoCommit(true);
        session.put(tableName, connection);

        return connection;
    }

    @Override
    public synchronized void endSession(String tableName) throws Exception {
        if (session.containsKey(tableName)) {
            session.get(tableName).close();
            session.remove(tableName);
        }
    }


    //DATA

    @Override
    public synchronized StoreStatus create(Table tableStructure, Date timeout) throws Exception {
        String tableName = tableStructure!=null ? tableStructure.getTableName() : null;
        if (session.containsKey(tableName))
            throw new ConcurrentModificationException("Write session already open on: "+tableName);
        Collection<Column> columns = tableStructure!=null ? tableStructure.getColumns() : null;
        if (tableName==null || columns==null || columns.size()==0)
            throw new Exception("Invalid table structure.");

        //Check if table exists
        if (loadMetadata().containsKey(tableName))
            throw new Exception("Duplicate table error.");

        //Create query
        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(getTableName(tableName)).append(" (");
        StringBuilder queryIndex = new StringBuilder(" PRIMARY KEY (");
        boolean containsKey = false;

        for (Column column : columns) {
            query.append(column.getName());

            switch (column.getType()) {
                case bool:      query.append(" BOOLEAN"); break;
                case real:      query.append(" DOUBLE PRECISION"); break;
                case string:    query.append(" VARCHAR"); break;
                case array:    query.append(" ARRAY"); break;
                case object:    query.append(" OTHER"); break;
                case integer:   query.append(" BIGINT"); break;
            }

            query.append(',');

            if (column.isKey()) {
                containsKey=true;
                queryIndex.append(column.getName()).append(',');
            }

        }
        if (containsKey) {
            queryIndex.setCharAt(queryIndex.length() - 1, ')');
            query.append(queryIndex).append(')');
        } else
            query.append(')');

        //Execute query and update metadata
        StoreStatus status = new StoreStatus(StoreStatus.Status.loading, 0l, new Date(), timeout);
        Connection connection = getConnection();
        try {
            storeMetadata(tableName, status, connection);
            connection.createStatement().executeUpdate(query.toString());

            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            loadMetadata().remove(tableName);
            ex.printStackTrace();
            throw ex;
        } finally {
            if (connection!=null)
                connection.close();
        }

        return status;
    }

    @Override
    public synchronized void delete(String tableName) throws Exception {
        if (session.containsKey(tableName))
            throw new ConcurrentModificationException("Write session already open on: "+tableName);

        Connection connection = getConnection();
        try {
            removeMetadata(tableName, connection);
            connection.createStatement().executeUpdate("DROP TABLE IF EXISTS " + getTableName(tableName));

            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            if (connection!=null)
                connection.close();
        }
    }

    private static final int MAX_PAGE_SIZE = 100000;

    @Override
    public Iterator<Object[]> load(Order ordering, Page pagination, DataFilter filter, Table... tables) throws Exception {
        Collection<ResultSet> data = new LinkedList<>();
        Collection<Object[]> defaults = new LinkedList<>();
        Connection connection = getConnection();
        //Select data
        if (tables!=null && tables.length>0)
            try {
                for (Table structure : tables)
                    for (ResultSet resultSet : load(connection, ordering, pagination, filter, structure)) {
                        data.add(resultSet);
                        defaults.add(structure.getNoDataValues());
                    }
            } catch (Exception ex) {
                if (!connection.isClosed())
                    connection.close();
                throw ex;
            }
        //Create and return data iterator
        return data.size()>0 ? new DataIterator(data,connection,10000l,defaults) : null;
    }

    private Collection<ResultSet> load(Connection connection, Order ordering, Page pagination, DataFilter filter, Table table) throws Exception {
        //Check table
        StoreStatus status = loadMetadata(table!=null ? table.getTableName() : null);
        if (status==null || status.getStatus()==StoreStatus.Status.incomplete)
            return null;

        int size = pagination!=null && pagination.getLength()<Integer.MAX_VALUE && pagination.getLength()>0 ? pagination.getLength() : status.getCount().intValue();

        Collection<ResultSet> resultList = new LinkedList<>();
        for (int skip = pagination!=null ? pagination.getSkip() : 0, length = size>MAX_PAGE_SIZE ? MAX_PAGE_SIZE : size; length>0; length = (size-=MAX_PAGE_SIZE)>MAX_PAGE_SIZE ? MAX_PAGE_SIZE : size, skip+=MAX_PAGE_SIZE) {
            //Create query
            Collection<Object> params = new LinkedList<>();
            String query = createFilterQuery(ordering, new Page(skip,length), filter, table, params, false);
            //Execute query
            PreparedStatement statement = connection.prepareStatement(query.toString());
            int i=1;
            for (Object param : params)
                statement.setObject(i++, param);
            //Return data
            resultList.add(statement.executeQuery());
        }
        return resultList;
    }

    @Override
    public synchronized StoreStatus store(Table tableMetadata, Iterator<Object[]> data, int size, boolean overwrite, Date referenceDate) throws Exception {
        String tableName = tableMetadata!=null ? tableMetadata.getTableName() : null;
        StoreStatus status = loadMetadata(tableName);
        if (status==null)
            throw new Exception("Unavailable table: "+tableName);

        boolean sessionOpen = session.containsKey(tableName);
        Connection connection = sessionOpen ? session.get(tableName) : getConnection();
        try {
            //Take action for 'incomplete' status
            if (status.getStatus()==StoreStatus.Status.incomplete) {
                //Try to skip with incomplete resources in append mode
                if (!overwrite)
                    try {
                        ((org.fao.fenix.commons.utils.database.Iterator)data).skip(status.getCount());
                    } catch (Exception ex) {
                        //If skip is unsupported force overwrite mode
                        status.setCount(0l);
                        overwrite = true;
                    }
                status.setStatus(StoreStatus.Status.loading);
                storeMetadata(tableName, status, connection);
            }
            //Retrieve table structure
            Column[] structure = tableMetadata.getColumns().toArray(new Column[tableMetadata.getColumns().size()]);
            int[] columnsType = new int[structure.length];
            for (int i=0; i<structure.length; i++)
                columnsType[i] = structure[i].getType().getSqlType();

            //If overwrite mode is active delete existing data
            if (overwrite)
                connection.createStatement().executeUpdate("DELETE FROM " + getTableName(tableName));

            //Check primary key availability
            boolean hasPrimaryKey = connection.getMetaData().getPrimaryKeys(null,null,getTableName(tableName)).next();

            //Build query
            StringBuilder query = new StringBuilder(overwrite || !hasPrimaryKey ? "INSERT INTO " : "MERGE INTO ").append(getTableName(tableName)).append(" (");

            for (Column column : structure)
                query.append(column.getName()).append(',');
            query.setLength(query.length()-1);

            query.append(") VALUES (");
            for (int count = structure.length; count>0; count--)
                query.append("?,");
            query.setLength(query.length()-1);
            query.append(')');

            //Prepare store session
            PreparedStatement statement = connection.prepareStatement(query.toString());
            size = size>0 ? size : Integer.MAX_VALUE;
            int count = 0;

            //Store data
            for (; count<size && data.hasNext(); count++) {
                Object[] row = data.next();
                for (int i=0; i<columnsType.length; i++)
                    statement.setObject(i+1,row[i],columnsType[i]);
                statement.addBatch();
            }
            statement.executeBatch();

            //Update status
            status.setStatus(data.hasNext() ? StoreStatus.Status.loading : StoreStatus.Status.ready);
            status.setCount(overwrite ? count : status.getCount() + count);
            status.setLastUpdate(new Date());
            storeMetadata(tableName, status, connection);

            //Close transaction
            if (!sessionOpen)
                connection.commit();
        } catch (Exception ex) {
            if (sessionOpen)
                connection.close();
            else
                connection.rollback();
            //try to set incomplete status
            status.setStatus(StoreStatus.Status.incomplete);
            status.setLastUpdate(referenceDate!=null ? referenceDate : new Date());
            storeMetadata(tableName, status);
            //throw error
            ex.printStackTrace();
            throw ex;
        } finally {
            if (!sessionOpen && connection!=null)
                connection.close();
        }

        return status;
    }

    @Override
    public synchronized StoreStatus store(Table table, DataFilter filter, boolean overwrite, Date referenceDate, Table... tables) throws Exception {
        String tableName = table!=null ? table.getTableName() : null;
        StoreStatus status = loadMetadata(tableName);
        if (status==null)
            throw new Exception("Unavailable table: "+tableName);

        //Prepare queries
        String[] queries = new String[tables.length];
        Collection<Object>[] params = new Collection[tables.length];
        String[] insertQueriesColumnsSegment = new String[tables.length];

        for (int i=0; i<tables.length; i++) {
            queries[i] = createFilterQuery(null, null, filter, tables[i], params[i] = new LinkedList<>(), true);

            StringBuilder segment = new StringBuilder();
            for (Column column : tables[i].getColumns())
                segment.append(',').append(column.getName());
            insertQueriesColumnsSegment[i] = segment.substring(1);
        }

        //Execute queries to populate destination table
        Connection connection = getConnection();
        try {
            int count = 0;
            //Insert data
            for (int i=0; i<tables.length; i++) {
                //Build insert query
                StringBuilder query = new StringBuilder(overwrite ? "INSERT INTO " : "MERGE INTO ")
                        .append(getTableName(tableName))
                        .append(" (").append(insertQueriesColumnsSegment[i]).append(") ")
                        .append(queries[i]);
                //Run query
                PreparedStatement statement = connection.prepareStatement(query.toString());
                int columnIndex=1;
                for (Object param : params[i])
                    statement.setObject(columnIndex++, param);
                count += statement.executeUpdate();
            }
            //Update status
            status.setStatus(StoreStatus.Status.ready);
            status.setCount(overwrite ? count : status.getCount() + count);
            status.setLastUpdate(referenceDate!=null ? referenceDate : new Date());
            storeMetadata(tableName, status, connection);
            //Commit changes
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            ex.printStackTrace();
            throw ex;
        } finally {
            if (connection!=null)
                connection.close();
        }

        //Return status
        return status;
    }



    //METADATA
    private Map<String, StoreStatus> metadata;

    @Override
    public Map<String, StoreStatus> loadMetadata() throws Exception {
        if (metadata==null) {
            Connection connection = getConnection();
            try {
                metadata = new HashMap<>();
                ResultSet result = connection.createStatement().executeQuery("SELECT id, status, rowsCount, lastUpdate, timeout FROM Metadata");
                while (result.next())
                    metadata.put(result.getString(1), new StoreStatus(StoreStatus.Status.valueOf(result.getString(2)), result.getLong(3), result.getTimestamp(4), result.getTimestamp(5)));
            } catch (Exception ex) {
                metadata = null;
                ex.printStackTrace();
                throw ex;
            } finally {
                if (connection!=null)
                    connection.close();
            }
        }
        return metadata;
    }

    @Override
    public StoreStatus loadMetadata(String resourceId) throws Exception {
        return loadMetadata().get(resourceId);
    }

    @Override
    public void storeMetadata(String resourceId, StoreStatus status) throws Exception {
        Connection connection = getConnection();
        try {
            storeMetadata(resourceId,status,connection);
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            ex.printStackTrace();
            throw ex;
        } finally {
            if (connection!=null)
                connection.close();
        }
    }
    private synchronized void storeMetadata(String resourceId, StoreStatus status, Connection connection) throws Exception {
        PreparedStatement statement = connection.prepareStatement(loadMetadata().put(resourceId, status) == null ?
                        "INSERT INTO Metadata (status, rowsCount, lastUpdate, timeout, id) VALUES (?,?,?,?,?)" :
                        "UPDATE Metadata SET status=?, rowsCount=?, lastUpdate=?, timeout=? WHERE id=?"
        );

        statement.setString(1,status.getStatus().name());
        if (status.getCount()!=null)
            statement.setLong(2, status.getCount());
        statement.setTimestamp(3, new Timestamp(status.getLastUpdate().getTime()));
        if (status.getTimeout()!=null)
            statement.setTimestamp(4, new Timestamp(status.getTimeout().getTime()));
        else
            statement.setNull(4, Types.TIMESTAMP);
        statement.setString(5, resourceId);

        statement.executeUpdate();
    }

    @Override
    public void removeMetadata(String resourceId) throws Exception {
        Connection connection = getConnection();
        try {
            removeMetadata(resourceId, connection);
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            ex.printStackTrace();
            throw ex;
        } finally {
            if (connection!=null)
                connection.close();
        }

    }
    public synchronized void removeMetadata(String resourceId, Connection connection) throws Exception {
        Map<String, StoreStatus> metadata = loadMetadata();
        if (metadata.containsKey(resourceId)) {
            connection.createStatement().executeUpdate("DELETE FROM Metadata WHERE id='" + resourceId + '\'');
            metadata.remove(resourceId);
        }
    }

    @Override
    public synchronized void storeMetadata(Map<String, StoreStatus> metadata, boolean overwrite) throws Exception {
        Connection connection = getConnection();
        try {
            Set<String> existingMetadata = loadMetadata().keySet();

            PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO Metadata (status, rowsCount, lastUpdate, timeout, id) VALUES (?,?,?,?,?)");
            PreparedStatement updateStatement = connection.prepareStatement("UPDATE Metadata SET status=?, rowsCount=?, lastUpdate=?, timeout=? WHERE id=?");
            for (Map.Entry<String, StoreStatus> statusEntry : metadata.entrySet()) {
                StoreStatus status = statusEntry.getValue();

                PreparedStatement statement = existingMetadata.remove(statusEntry.getKey()) ? updateStatement : insertStatement;
                statement.setString(1, status.getStatus().name());
                if (status.getCount() != null)
                    statement.setLong(2, status.getCount());
                statement.setTimestamp(3, new Timestamp(status.getLastUpdate().getTime()));
                if (status.getTimeout() != null)
                    statement.setTimestamp(4, new Timestamp(status.getTimeout().getTime()));
                statement.setString(5, statusEntry.getKey());

                statement.addBatch();
            }
            insertStatement.executeBatch();
            updateStatement.executeBatch();

            if (overwrite && existingMetadata.size() > 0) {
                StringBuilder deleteQuery = new StringBuilder();
                for (String id : existingMetadata)
                    deleteQuery.append(",\"").append(id).append('"');
                connection.createStatement().executeUpdate("DELETE FROM Metadata WHERE id IN (" + deleteQuery.substring(1) + ')');
            }

            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            ex.printStackTrace();
            throw ex;
        } finally {
            this.metadata = null; //Force metadata reload
            if (connection!=null)
                connection.close();
        }

    }


    //MAINTENANCE
    @Override
    public synchronized int clean() throws Exception {
        Connection connection = getConnection();
        int count=0;
        try {
            Set<String> tablesName = new HashSet<>();
            for (ResultSet tables=connection.getMetaData().getTables(null,SCHEMA_NAME.toUpperCase(),null,null); tables.next(); )
                tablesName.add(tables.getString("TABLE_NAME"));
            Map<String,StoreStatus> statusMap = loadMetadata();

            for (Map.Entry<String,StoreStatus> statusEntry : statusMap.entrySet()) {
                StoreStatus status = statusEntry.getValue();
                String tableName = statusEntry.getKey();
                Date timeout = status.getTimeout();

                //Remove incomplete and timed out tables
                if (status.getStatus()==StoreStatus.Status.incomplete || timeout!=null && timeout.compareTo(new Date())>0) {
                    delete(tableName);
                    count++;
                }

                //Remove orphan metadata
                if (!tablesName.contains(tableName)) {
                    removeMetadata(tableName);
                    count++;
                } else
                    tablesName.remove(tableName);
            }

            //Remove orphan tables
            for (String tableName : tablesName) {
                connection.createStatement().executeUpdate("DROP TABLE " + getTableName(tableName));
                count++;
            }

        } catch (Exception ex) {
            connection.rollback();
            ex.printStackTrace();
            throw ex;
        } finally {
            this.metadata = null; //Force metadata reload
            connection.close();
        }

        return count;
    }

    @Override
    public synchronized void reset() throws Exception {
        for (String id : loadMetadata().keySet())
            delete(id);
    }


    //Utils
    private String createFilterQuery(Order ordering, Page pagination, DataFilter filter, Table table, Collection<Object> params, boolean forUpdate) throws Exception {
        Map<String, Column> columnsByName = table.getColumnsByName();

        StringBuilder query = new StringBuilder("SELECT ");

        //Add select columns
        Collection<String> selectColumns = filter!=null ? filter.getColumns() : null;
        if (selectColumns!=null && selectColumns.size()>0) {
            selectColumns.retainAll(columnsByName.keySet()); //use only existing columns
            for (String name : selectColumns)
                query.append(name).append(',');
            query.setLength(query.length()-1);
        } else {
            for (Column column : table.getColumns())
                if (column.getType()==Type.array)
                    query.append("ARRAY_GET (").append(column.getName()).append(",1), ");
                else
                    query.append(column.getName()).append(", ");
            query.setLength(query.length()-2);
        }

        //Add source table
        query.append(" FROM ").append(getTableName(table.getTableName()));
        //Add where condition
        String whereContitionSQL = null;
        StandardFilter rowsFilter = filter!=null ? filter.getRows() : null;
        if (rowsFilter!=null && rowsFilter.size()>0) {
            StringBuilder whereContition = new StringBuilder();
            for (Map.Entry<String, FieldFilter> conditionEntry : rowsFilter.entrySet()) {
                String fieldName = conditionEntry.getKey();
                Column column = columnsByName.get(fieldName);
                FieldFilter fieldFilter = conditionEntry.getValue();

                if (column==null)
                    throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);

                Type columnType = column.getType();
                if (fieldFilter!=null) {
                    switch (fieldFilter.retrieveFilterType()) {
                        case enumeration:
                            if (columnType!=Type.string)
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                            whereContition.append(" AND ").append(fieldName).append(" IN (");
                            for (String value : fieldFilter.enumeration) {
                                whereContition.append("?,");
                                params.add(value);
                            }
                            whereContition.setCharAt(whereContition.length() - 1, ')');
                            break;
                        case time:
                            if (columnType!=Type.integer)
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                            whereContition.append(" AND (");
                            for (TimeFilter timeFilter : fieldFilter.time) {
                                if (timeFilter.from!=null) {
                                    whereContition.append(fieldName).append(" >= ?");
                                    params.add(timeFilter.getFrom(column.getPrecision()));
                                }
                                if (timeFilter.to!=null) {
                                    if (timeFilter.from!=null)
                                        whereContition.append(" AND ");
                                    whereContition.append(fieldName).append(" <= ?");
                                    params.add(timeFilter.getTo(column.getPrecision()));
                                }
                                whereContition.append(" OR ");
                            }
                            whereContition.setLength(whereContition.length()-4);
                            whereContition.append(')');
                            break;
                        case code:
                            whereContition.append(" AND ");
                            if (columnType==Type.string) {
                                whereContition.append(fieldName).append(" IN (");
                                for (CodesFilter codesFilter : fieldFilter.codes)
                                    for (String code : codesFilter.codes) {
                                        whereContition.append("?,");
                                        params.add(code);
                                    }
                                whereContition.setCharAt(whereContition.length()-1, ')');
                            } else if (columnType==Type.array) {
                                whereContition.append('(');
                                for (CodesFilter codesFilter : fieldFilter.codes)
                                    for (String code : codesFilter.codes) {
                                        whereContition.append("ARRAY_CONTAINS (").append(fieldName).append(", ?) OR ");
                                        params.add(code);
                                    }
                                whereContition.setLength(whereContition.length()-4);
                                whereContition.append(')');
                            } else
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                    }

                }
            }
            query.append (" WHERE ").append(whereContitionSQL = whereContition.substring(4));
        }


        //Add ordering
        String orderingSQL = null;
        if (ordering!=null)
            query.append(orderingSQL = ordering.toH2SQL(columnsByName.keySet().toArray(new String[columnsByName.size()])));

        //Add pagination
        if (pagination!=null)
            if (pagination.toH2SQLWhereCondition()!=null && (orderingSQL==null || orderingSQL.trim().length()==0) && (whereContitionSQL==null || whereContitionSQL.trim().length()==0))
                query.append(" WHERE ").append(pagination.toH2SQLWhereCondition());
            else
                query.append(' ').append(pagination.toH2SQL());

        //Add for update option
        if (forUpdate)
            query.append(" FOR UPDATE");

        //Return query
        return query.toString();
    }

    @Override
    public String getTableName(String tableName) {
        return tableName==null ? null : SCHEMA_NAME + '.'+ '"' + tableName + '"';
    }


}










/*
    //Manage default table indexes
    private void dropIndexes(Table tableStructure) throws Exception {
        String tableName = tableStructure!=null ? tableStructure.getTableName() : null;
        Collection<Column> columns = tableStructure!=null ? tableStructure.getColumns() : null;
        if (columns==null)
            throw new Exception("Missing table structure");

        int keyColumnsCount = 0;
        boolean containsKey = false;

        for (Column column : columns)
            if (column.isKey()) {
                containsKey=true;
                keyColumnsCount++;
            }

        if (containsKey) {
            Connection connection = getConnection();
            try {
                String baseName = "INDEX_"+tableName+'_';
                for (int i=1; i<keyColumnsCount; i++)
                    connection.createStatement().executeUpdate("DROP INDEX IF EXISTS "+baseName+i);
                connection.commit();
            } catch (Exception ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.close();
            }
        }
    }
    private void createIndexes(Table tableStructure) throws Exception {
        String tableName = tableStructure!=null ? tableStructure.getTableName() : null;
        Collection<Column> columns = tableStructure!=null ? tableStructure.getColumns() : null;
        if (columns==null)
            throw new Exception("Missing table structure");

        ArrayList<String> keyColumns = new ArrayList<>();
        boolean containsKey = false;

        for (Column column : columns)
            if (column.isKey()) {
                containsKey=true;
                keyColumns.add(column.getName());
            }

        if (containsKey) {
            keyColumns.remove(0); //skip first column
            Connection connection = getConnection();
            try {
                for (int i=1; keyColumns.size()>0; i++) {
                    StringBuilder query = new StringBuilder("CREATE INDEX IF NOT EXISTS INDEX_").append(tableName).append('_').append(i).append(" ON ").append(getTableName(tableName)).append(" (");
                    for (String keyColumn : keyColumns)
                        query.append(keyColumn).append(',');
                    query.setCharAt(query.length() - 1, ')');

                    connection.createStatement().executeUpdate(query.toString());

                    keyColumns.remove(0);
                }
                connection.commit();
            } catch (Exception ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.close();
            }
        }
    }
*/
