package com.symphony.hackathon.bot.topusers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * </p>Created by Ivan Rylach on 4/6/17
 * @author i@symphony.com
 */
public class BotApp {

  private final static Logger log = LoggerFactory.getLogger(BotApp.class);
  private static final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private static final Integer BOT_PUBLISH_PERIOD =
      Integer.parseInt(System.getProperty("bot.topusers.publish.period.sec", "30"));
  private static SymphonyService symphonyService;
  private static DataSourceService dataSourceService;

  public static void main(String[] args) {
    try {
      symphonyService = new SymphonyService();
      dataSourceService = new DataSourceService();

      Beacon beacon = new Beacon(symphonyService, dataSourceService);

      executorService.scheduleAtFixedRate(beacon, BOT_PUBLISH_PERIOD, BOT_PUBLISH_PERIOD,
          TimeUnit.SECONDS);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            executorService.shutdown();
            symphonyService.shutdown();
            dataSourceService.shutdown();
          } catch (Exception e) {
            log.error("Failed to shutdown gracefully.", e);
          }
        }
      });


    } catch (Exception e) {
      log.error("Something terrible just happened...", e);
    }
  }

}
