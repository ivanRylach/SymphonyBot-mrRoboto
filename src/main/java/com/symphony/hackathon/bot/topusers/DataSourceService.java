package com.symphony.hackathon.bot.topusers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * </p>Created by Ivan Rylach on 4/6/17
 * @author i@symphony.com
 */
class DataSourceService extends Configurable {

  private final static Logger log = LoggerFactory.getLogger(DataSourceService.class);
  private Map<String, String> namesDictionary;

  private static Set<String> initParamNames = new HashSet<>();

  public static final String SQL_ACTIVE_USER = "SELECT userid, COUNT(event) AS value\n"
      + "FROM message_sent\n"
      + "WHERE sendertype = 'END_USER'\n"
      + "GROUP BY userid\n"
      + "ORDER BY value DESC\n"
      + "LIMIT 1;";

  public static final String SQL_ACTIVE_BOT = "SELECT userid, COUNT(event) AS value\n"
      + "FROM message_sent\n"
      + "WHERE sendertype = 'SERVICE_USER'\n"
      + "GROUP BY userid\n"
      + "ORDER BY value DESC\n"
      + "LIMIT 1;\n";

  public static final String SQL_BIGGEST_BLASTER = "SELECT userid, SUM(messagefan) AS value\n"
      + "FROM message_sent\n"
      + "WHERE sendertype = 'END_USER'\n"
      + "AND streamtype = 'CHATROOM'\n"
      + "GROUP BY userid\n"
      + "ORDER BY value DESC\n"
      + "LIMIT 1;\n";

  public static final String SQL_AVID_CHAT_ROOM_USER = "SELECT userid, COUNT(DISTINCT streamid) "
      + "AS value\n"
      + "FROM message_read\n"
      + "WHERE timestamp BETWEEN CURRENT_DATE AND DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)\n"
      + "AND streamtype = 'CHATROOM'\n"
      + "GROUP BY userid\n"
      + "ORDER BY vlaue DESC\n"
      + "LIMIT 1;\n";
  public static final String SQL_SECRET_CONFIDENT = "SELECT userid, COUNT(DISTINCT streamid) AS "
      + "value\n"
      + "FROM message_sent\n"
      + "WHERE timestamp BETWEEN CURRENT_DATE AND DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)\n"
      + "AND streamtype = 'INSTANT_CHAT'\n"
      + "AND messagefan = 2\n"
      + "GROUP BY userid\n"
      + "ORDER BY value DESC\n"
      + "LIMIT 1;\n";
  public static final String SQL_SOCIAL_NETWORKER = "SELECT userid, COUNT(event) AS value\n"
      + "FROM connectionrequest_sent\n"
      + "WHERE timestamp BETWEEN CURRENT_DATE AND DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)\n"
      + "GROUP BY userid\n"
      + "ORDER BY value DESC\n"
      + "LIMIT 1;\n";
  public static final String SQL_GIF_CRAZY = "SELECT userid, COUNT(event) AS value\n"
      + "FROM attachment_sent\n"
      + "WHERE timestamp BETWEEN CURRENT_DATE AND DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)\n"
      + "AND fileextension = '.gif'\n"
      + "GROUP BY userid\n"
      + "ORDER BY value DESC\n"
      + "LIMIT 1;\n";

  static {
    initParamNames.add("bot.topusers.datasource.db.url");
    initParamNames.add("bot.topusers.datasource.username");
    initParamNames.add("bot.topusers.datasource.password");
    initParamNames.add("bot.topusers.datasource.names.file");
  }

  private Connection connection;

  DataSourceService() throws ClassNotFoundException, SQLException, IOException {
    log.info("Reading configurations for data source...");
    initParams(initParamNames);
    namesDictionary = getMapFromCSV(initParams.get("bot.topusers.datasource.names.file"));
    log.info("Configurations for data source have been read.");
    log.info("Setting connection to data source up...");
    initJDBC();
    log.info("Connection to data source was set up.");
  }

  DBResult getActiveUser() throws SQLException {
    return executeQuery(SQL_ACTIVE_USER);
  }

  DBResult getActiveBot() throws SQLException {
    return executeQuery(SQL_ACTIVE_BOT);
  }

  DBResult getBiggestBlaster() throws SQLException {
    return executeQuery(SQL_BIGGEST_BLASTER);
  }

  DBResult getAvidChatRoomUser() throws SQLException {
    return executeQuery(SQL_AVID_CHAT_ROOM_USER);
  }

  DBResult getSecretConfident() throws SQLException {
    return executeQuery(SQL_SECRET_CONFIDENT);
  }

  DBResult getSocialNetworker() throws SQLException {
    return executeQuery(SQL_SOCIAL_NETWORKER);
  }

  DBResult getGifCrazy() throws SQLException {
    return executeQuery(SQL_GIF_CRAZY);
  }

  private DBResult executeQuery(String sqlQuery) throws SQLException {
    log.debug("Preparing SQL statement...");
    Statement sqlStatement = connection.createStatement();

    log.debug("Executing SQL statement...");
    ResultSet resultSet = sqlStatement.executeQuery(sqlQuery);

    long userId = 0;
    long value = 0;
    while (resultSet.next()) {
      userId = resultSet.getLong("userid");
      value = resultSet.getLong("value");
    }

    DBResult dbResult = new DBResult(userId, value, namesDictionary.get(Long.toString(userId)));

    resultSet.close();
    sqlStatement.close();
    return dbResult;
  }



  void shutdown() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }


  private void initJDBC() throws ClassNotFoundException, SQLException {
    Class.forName("com.amazon.redshift.jdbc42.Driver");
    Properties props = new Properties();
    props.setProperty("user", initParams.get("bot.topusers.datasource.username"));
    props.setProperty("password", initParams.get("bot.topusers.datasource.password"));
    String dbURL = initParams.get("bot.topusers.datasource.db.url");
    this.connection = DriverManager.getConnection(dbURL, props);
  }

  public static Map<String, String> getMapFromCSV(final String filePath) throws IOException {

    Stream<String> lines = Files.lines(Paths.get(filePath));
    Map<String, String> resultMap =
        lines.map(line -> line.split(","))
            .collect(Collectors.toMap(line -> line[0], line -> line[1]));

    lines.close();

    return resultMap;
  }

}
